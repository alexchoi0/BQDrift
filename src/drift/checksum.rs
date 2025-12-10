use sha2::{Sha256, Digest};
use crate::dsl::VersionDef;
use crate::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct Checksums {
    pub sql: String,
    pub schema: String,
    pub yaml: String,
}

impl Checksums {
    pub fn compute(
        sql_content: &str,
        schema: &Schema,
        yaml_content: &str,
    ) -> Self {
        Self {
            sql: Self::sha256(sql_content),
            schema: Self::sha256(&Self::schema_to_json(schema)),
            yaml: Self::sha256(yaml_content),
        }
    }

    pub fn from_version(
        version: &VersionDef,
        yaml_content: &str,
        execution_date: chrono::NaiveDate,
    ) -> Self {
        let sql = version.get_sql_for_date(execution_date);
        Self::compute(sql, &version.schema, yaml_content)
    }

    fn sha256(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    fn schema_to_json(schema: &Schema) -> String {
        serde_json::to_string(&schema.fields).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_deterministic() {
        let checksum1 = Checksums::sha256("hello world");
        let checksum2 = Checksums::sha256("hello world");
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_sha256_different_input() {
        let checksum1 = Checksums::sha256("hello");
        let checksum2 = Checksums::sha256("world");
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_compute_checksums() {
        let schema = Schema::default();
        let checksums = Checksums::compute("SELECT 1", &schema, "name: test");

        assert!(!checksums.sql.is_empty());
        assert!(!checksums.schema.is_empty());
        assert!(!checksums.yaml.is_empty());
    }
}
