use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use regex::Regex;
use crate::error::{BqDriftError, Result};

pub struct YamlPreprocessor {
    file_pattern: Regex,
}

impl YamlPreprocessor {
    pub fn new() -> Self {
        Self {
            file_pattern: Regex::new(r#"\$\{\{\s*file:\s*([^\s}]+)\s*\}\}"#).unwrap(),
        }
    }

    pub fn process(&self, content: &str, base_dir: &Path) -> Result<String> {
        let mut visited = HashSet::new();
        self.process_recursive(content, base_dir, &mut visited)
    }

    fn process_recursive(
        &self,
        content: &str,
        base_dir: &Path,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<String> {
        let mut result = String::new();
        let mut last_end = 0;

        for caps in self.file_pattern.captures_iter(content) {
            let full_match = caps.get(0).unwrap();
            let file_path = caps.get(1).unwrap().as_str();

            result.push_str(&content[last_end..full_match.start()]);

            let resolved_path = base_dir.join(file_path);
            let canonical = resolved_path.canonicalize()
                .map_err(|_| BqDriftError::FileInclude(
                    format!("File not found: {}", resolved_path.display())
                ))?;

            if visited.contains(&canonical) {
                return Err(BqDriftError::FileInclude(
                    format!("Circular include detected: {}", canonical.display())
                ));
            }
            visited.insert(canonical.clone());

            let included_content = fs::read_to_string(&canonical)
                .map_err(|_| BqDriftError::FileInclude(
                    format!("Failed to read: {}", canonical.display())
                ))?;

            let included_base = canonical.parent().unwrap_or(base_dir);
            let processed = self.process_recursive(&included_content, included_base, visited)?;

            let indent = self.detect_indent(content, full_match.start());
            let indented = self.apply_indent(&processed, &indent, full_match.start(), content);

            result.push_str(&indented);
            last_end = full_match.end();

            visited.remove(&canonical);
        }

        result.push_str(&content[last_end..]);
        Ok(result)
    }

    fn detect_indent(&self, content: &str, match_start: usize) -> String {
        let before = &content[..match_start];
        if let Some(line_start) = before.rfind('\n') {
            let line_prefix = &before[line_start + 1..];
            let indent: String = line_prefix.chars().take_while(|c| c.is_whitespace()).collect();
            indent
        } else {
            let indent: String = before.chars().take_while(|c| c.is_whitespace()).collect();
            indent
        }
    }

    fn apply_indent(&self, content: &str, indent: &str, match_start: usize, original: &str) -> String {
        let trimmed = content.trim();

        if !trimmed.contains('\n') {
            return trimmed.to_string();
        }

        let is_yaml_value = self.is_yaml_value_position(original, match_start);

        let lines: Vec<&str> = trimmed.lines().collect();
        if lines.is_empty() {
            return String::new();
        }

        if is_yaml_value && lines.len() > 1 {
            let mut result = String::from("|\n");
            for line in &lines {
                result.push_str(indent);
                result.push_str("  ");
                result.push_str(line);
                result.push('\n');
            }
            result.trim_end().to_string()
        } else {
            let mut result = String::new();
            for (i, line) in lines.iter().enumerate() {
                if i > 0 {
                    result.push('\n');
                    result.push_str(indent);
                }
                result.push_str(line);
            }
            result
        }
    }

    fn is_yaml_value_position(&self, content: &str, pos: usize) -> bool {
        let before = &content[..pos];
        if let Some(line_start) = before.rfind('\n') {
            let line = &before[line_start + 1..];
            line.contains(':') && line.trim_end().ends_with(':')
        } else {
            before.contains(':') && before.trim_end().ends_with(':')
        }
    }

    pub fn has_file_includes(&self, content: &str) -> bool {
        self.file_pattern.is_match(content)
    }

    pub fn extract_file_refs(&self, content: &str) -> Vec<String> {
        self.file_pattern
            .captures_iter(content)
            .map(|caps| caps.get(1).unwrap().as_str().to_string())
            .collect()
    }
}

impl Default for YamlPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[test]
    fn test_simple_file_include() {
        let dir = setup_test_dir();
        let schema_path = dir.path().join("schema.yaml");
        fs::write(&schema_path, "- name: id\n  type: INT64\n- name: name\n  type: STRING").unwrap();

        let preprocessor = YamlPreprocessor::new();
        let input = "schema: ${{ file: schema.yaml }}";
        let result = preprocessor.process(input, dir.path()).unwrap();

        assert!(result.contains("name: id"));
        assert!(result.contains("type: INT64"));
    }

    #[test]
    fn test_inline_sql_include() {
        let dir = setup_test_dir();
        let sql_path = dir.path().join("query.sql");
        fs::write(&sql_path, "SELECT * FROM table").unwrap();

        let preprocessor = YamlPreprocessor::new();
        let input = "source: ${{ file: query.sql }}";
        let result = preprocessor.process(input, dir.path()).unwrap();

        assert!(result.contains("SELECT * FROM table"));
    }

    #[test]
    fn test_multiline_sql_becomes_block_scalar() {
        let dir = setup_test_dir();
        let sql_path = dir.path().join("query.sql");
        fs::write(&sql_path, "SELECT *\nFROM table\nWHERE x = 1").unwrap();

        let preprocessor = YamlPreprocessor::new();
        let input = "source: ${{ file: query.sql }}";
        let result = preprocessor.process(input, dir.path()).unwrap();

        assert!(result.contains("|"));
        assert!(result.contains("SELECT *"));
    }

    #[test]
    fn test_nested_include() {
        let dir = setup_test_dir();

        let inner_path = dir.path().join("inner.yaml");
        fs::write(&inner_path, "- name: id\n  type: INT64").unwrap();

        let outer_path = dir.path().join("outer.yaml");
        fs::write(&outer_path, "fields: ${{ file: inner.yaml }}").unwrap();

        let preprocessor = YamlPreprocessor::new();
        let input = "schema: ${{ file: outer.yaml }}";
        let result = preprocessor.process(input, dir.path()).unwrap();

        assert!(result.contains("name: id"));
    }

    #[test]
    fn test_circular_include_detection() {
        let dir = setup_test_dir();

        let a_path = dir.path().join("a.yaml");
        let b_path = dir.path().join("b.yaml");

        fs::write(&a_path, "x: ${{ file: b.yaml }}").unwrap();
        fs::write(&b_path, "y: ${{ file: a.yaml }}").unwrap();

        let preprocessor = YamlPreprocessor::new();
        let input = "root: ${{ file: a.yaml }}";
        let result = preprocessor.process(input, dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Circular"));
    }

    #[test]
    fn test_file_not_found() {
        let dir = setup_test_dir();
        let preprocessor = YamlPreprocessor::new();
        let input = "schema: ${{ file: nonexistent.yaml }}";
        let result = preprocessor.process(input, dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_extract_file_refs() {
        let preprocessor = YamlPreprocessor::new();
        let input = r#"
schema: ${{ file: schema.yaml }}
source: ${{ file: query.sql }}
"#;
        let refs = preprocessor.extract_file_refs(input);
        assert_eq!(refs, vec!["schema.yaml", "query.sql"]);
    }

    #[test]
    fn test_preserves_indentation_in_list() {
        let dir = setup_test_dir();
        let schema_path = dir.path().join("schema.yaml");
        fs::write(&schema_path, "- name: a\n  type: INT64\n- name: b\n  type: STRING").unwrap();

        let preprocessor = YamlPreprocessor::new();
        let input = "versions:\n  - version: 1\n    schema: ${{ file: schema.yaml }}";
        let result = preprocessor.process(input, dir.path()).unwrap();

        assert!(result.contains("versions:"));
    }
}
