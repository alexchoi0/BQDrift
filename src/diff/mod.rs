use base64::{Engine, engine::general_purpose::STANDARD};
use colored::Colorize;
use similar::{ChangeTag, TextDiff};

pub fn encode_sql(sql: &str) -> String {
    STANDARD.encode(sql)
}

pub fn decode_sql(encoded: &str) -> Option<String> {
    STANDARD.decode(encoded)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

pub fn format_sql_diff(old_sql: &str, new_sql: &str) -> String {
    let diff = TextDiff::from_lines(old_sql, new_sql);
    let mut output = String::new();

    output.push_str(&"───────────────────────────────────────\n".dimmed().to_string());

    for change in diff.iter_all_changes() {
        let line = change.to_string();
        let formatted = match change.tag() {
            ChangeTag::Delete => format!("- {}", line.trim_end()).red().to_string(),
            ChangeTag::Insert => format!("+ {}", line.trim_end()).green().to_string(),
            ChangeTag::Equal => format!("  {}", line.trim_end()).to_string(),
        };
        output.push_str(&formatted);
        output.push('\n');
    }

    output.push_str(&"───────────────────────────────────────".dimmed().to_string());

    output
}

pub fn has_changes(old_sql: &str, new_sql: &str) -> bool {
    old_sql.trim() != new_sql.trim()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let sql = "SELECT * FROM table WHERE date = '2024-01-01'";
        let encoded = encode_sql(sql);
        let decoded = decode_sql(&encoded).unwrap();
        assert_eq!(sql, decoded);
    }

    #[test]
    fn test_encode_multiline() {
        let sql = "SELECT\n  date,\n  COUNT(*) as cnt\nFROM table";
        let encoded = encode_sql(sql);
        let decoded = decode_sql(&encoded).unwrap();
        assert_eq!(sql, decoded);
    }

    #[test]
    fn test_has_changes_true() {
        let old = "SELECT user_id FROM users";
        let new = "SELECT COALESCE(user_id, 'anon') FROM users";
        assert!(has_changes(old, new));
    }

    #[test]
    fn test_has_changes_false_whitespace() {
        let old = "SELECT * FROM users  ";
        let new = "SELECT * FROM users";
        assert!(!has_changes(old, new));
    }

    #[test]
    fn test_format_diff_shows_changes() {
        let old = "SELECT\n  user_id\nFROM users";
        let new = "SELECT\n  COALESCE(user_id, 'anon')\nFROM users";
        let diff = format_sql_diff(old, new);
        assert!(diff.contains("user_id"));
        assert!(diff.contains("COALESCE"));
    }
}
