use crate::schema::BqType;
use super::parser::QueryDef;

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub query_name: String,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
}

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ValidationWarning {
    pub code: &'static str,
    pub message: String,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

pub struct QueryValidator;

impl QueryValidator {
    pub fn validate(query: &QueryDef) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        Self::check_partition_field(query, &mut errors);
        Self::check_cluster_fields(query, &mut errors);
        Self::check_duplicate_versions(query, &mut errors);
        Self::check_record_fields(query, &mut errors);
        Self::check_effective_from_order(query, &mut warnings);
        Self::check_duplicate_revisions(query, &mut warnings);
        Self::check_schema_breaking_changes(query, &mut warnings);
        Self::check_sql_partition_placeholder(query, &mut warnings);
        Self::check_empty_schema(query, &mut warnings);

        ValidationResult {
            query_name: query.name.clone(),
            errors,
            warnings,
        }
    }

    fn check_partition_field(query: &QueryDef, errors: &mut Vec<ValidationError>) {
        if let Some(ref partition_field) = query.destination.partition.field {
            for version in &query.versions {
                if !version.schema.has_field(partition_field) {
                    errors.push(ValidationError {
                        code: "E001",
                        message: format!(
                            "v{}: partition field '{}' not found in schema",
                            version.version, partition_field
                        ),
                    });
                }
            }
        }
    }

    fn check_cluster_fields(query: &QueryDef, errors: &mut Vec<ValidationError>) {
        if let Some(ref cluster) = query.cluster {
            for version in &query.versions {
                for field in &cluster.fields {
                    if !version.schema.has_field(field) {
                        errors.push(ValidationError {
                            code: "E002",
                            message: format!(
                                "v{}: cluster field '{}' not found in schema",
                                version.version, field
                            ),
                        });
                    }
                }
            }
        }
    }

    fn check_duplicate_versions(query: &QueryDef, errors: &mut Vec<ValidationError>) {
        let mut seen = std::collections::HashSet::new();
        for version in &query.versions {
            if !seen.insert(version.version) {
                errors.push(ValidationError {
                    code: "E003",
                    message: format!("duplicate version number: {}", version.version),
                });
            }
        }
    }

    fn check_record_fields(query: &QueryDef, errors: &mut Vec<ValidationError>) {
        for version in &query.versions {
            for field in &version.schema.fields {
                Self::check_record_field_recursive(field, version.version, errors);
            }
        }
    }

    fn check_record_field_recursive(field: &crate::schema::Field, version: u32, errors: &mut Vec<ValidationError>) {
        if field.field_type == BqType::Record {
            match &field.fields {
                None => {
                    errors.push(ValidationError {
                        code: "E004",
                        message: format!(
                            "v{}: RECORD field '{}' must have nested fields defined",
                            version, field.name
                        ),
                    });
                }
                Some(nested) if nested.is_empty() => {
                    errors.push(ValidationError {
                        code: "E004",
                        message: format!(
                            "v{}: RECORD field '{}' has empty nested fields",
                            version, field.name
                        ),
                    });
                }
                Some(nested) => {
                    for f in nested {
                        Self::check_record_field_recursive(f, version, errors);
                    }
                }
            }
        }
    }

    fn check_effective_from_order(query: &QueryDef, warnings: &mut Vec<ValidationWarning>) {
        let mut sorted = query.versions.clone();
        sorted.sort_by_key(|v| v.version);

        for window in sorted.windows(2) {
            let prev = &window[0];
            let curr = &window[1];
            if curr.effective_from < prev.effective_from {
                warnings.push(ValidationWarning {
                    code: "W001",
                    message: format!(
                        "v{} effective_from ({}) is before v{} ({})",
                        curr.version, curr.effective_from, prev.version, prev.effective_from
                    ),
                });
            }
        }
    }

    fn check_duplicate_revisions(query: &QueryDef, warnings: &mut Vec<ValidationWarning>) {
        for version in &query.versions {
            let mut seen = std::collections::HashSet::new();
            for revision in &version.revisions {
                if !seen.insert(revision.revision) {
                    warnings.push(ValidationWarning {
                        code: "W002",
                        message: format!(
                            "v{}: duplicate revision number: {}",
                            version.version, revision.revision
                        ),
                    });
                }
            }
        }
    }

    fn check_schema_breaking_changes(query: &QueryDef, warnings: &mut Vec<ValidationWarning>) {
        let mut sorted = query.versions.clone();
        sorted.sort_by_key(|v| v.version);

        for window in sorted.windows(2) {
            let prev = &window[0];
            let curr = &window[1];

            // Check for removed fields
            for field in &prev.schema.fields {
                if !curr.schema.has_field(&field.name) {
                    warnings.push(ValidationWarning {
                        code: "W003",
                        message: format!(
                            "v{}: field '{}' was removed (breaking change from v{})",
                            curr.version, field.name, prev.version
                        ),
                    });
                }
            }

            // Check for type changes
            for prev_field in &prev.schema.fields {
                if let Some(curr_field) = curr.schema.get_field(&prev_field.name) {
                    if prev_field.field_type != curr_field.field_type {
                        warnings.push(ValidationWarning {
                            code: "W004",
                            message: format!(
                                "v{}: field '{}' type changed from {:?} to {:?}",
                                curr.version, prev_field.name,
                                prev_field.field_type, curr_field.field_type
                            ),
                        });
                    }
                }
            }
        }
    }

    fn check_sql_partition_placeholder(query: &QueryDef, warnings: &mut Vec<ValidationWarning>) {
        for version in &query.versions {
            if !version.sql_content.contains("@partition_date")
                && !version.sql_content.contains("@run_date")
                && !version.sql_content.contains("@execution_date") {
                warnings.push(ValidationWarning {
                    code: "W005",
                    message: format!(
                        "v{}: SQL does not contain @partition_date placeholder",
                        version.version
                    ),
                });
            }

            for revision in &version.revisions {
                if !revision.sql_content.contains("@partition_date")
                    && !revision.sql_content.contains("@run_date")
                    && !revision.sql_content.contains("@execution_date") {
                    warnings.push(ValidationWarning {
                        code: "W005",
                        message: format!(
                            "v{}.r{}: SQL does not contain @partition_date placeholder",
                            version.version, revision.revision
                        ),
                    });
                }
            }
        }
    }

    fn check_empty_schema(query: &QueryDef, warnings: &mut Vec<ValidationWarning>) {
        for version in &query.versions {
            if version.schema.fields.is_empty() {
                warnings.push(ValidationWarning {
                    code: "W006",
                    message: format!("v{}: schema has no fields", version.version),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::QueryLoader;
    use std::path::Path;

    #[test]
    fn test_validate_simple_query() {
        let loader = QueryLoader::new();
        let query = loader.load_query(Path::new("tests/fixtures/analytics/simple_query.yaml")).unwrap();
        let result = QueryValidator::validate(&query);

        assert!(result.is_valid());
    }

    #[test]
    fn test_validate_versioned_query() {
        let loader = QueryLoader::new();
        let query = loader.load_query(Path::new("tests/fixtures/analytics/versioned_query.yaml")).unwrap();
        let result = QueryValidator::validate(&query);

        assert!(result.is_valid());
    }
}
