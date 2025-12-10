use sha2::{Sha256, Digest};
use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use std::io::{Write, Read};
use crate::dsl::VersionDef;
use crate::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct Checksums {
    pub sql: String,
    pub schema: String,
    pub yaml: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionArtifact {
    pub sql_checksum: String,
    pub sql_compressed: String,
    pub schema_checksum: String,
    pub yaml_checksum: String,
    pub yaml_compressed: String,
}

impl Checksums {
    pub fn compute(
        sql_content: &str,
        schema: &Schema,
        yaml_content: &str,
    ) -> Self {
        Self {
            sql: Self::sha256(&compress_to_base64(sql_content)),
            schema: Self::sha256(&compress_to_base64(&Self::schema_to_json(schema))),
            yaml: Self::sha256(&compress_to_base64(yaml_content)),
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

    pub fn sha256(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    fn schema_to_json(schema: &Schema) -> String {
        serde_json::to_string(&schema.fields).unwrap_or_default()
    }
}

impl ExecutionArtifact {
    pub fn create(
        sql_content: &str,
        schema: &Schema,
        yaml_content: &str,
    ) -> Self {
        let sql_compressed = compress_to_base64(sql_content);
        let schema_compressed = compress_to_base64(&serde_json::to_string(&schema.fields).unwrap_or_default());
        let yaml_compressed = compress_to_base64(yaml_content);

        Self {
            sql_checksum: Checksums::sha256(&sql_compressed),
            sql_compressed,
            schema_checksum: Checksums::sha256(&schema_compressed),
            yaml_checksum: Checksums::sha256(&yaml_compressed),
            yaml_compressed,
        }
    }

    pub fn from_version(
        version: &VersionDef,
        yaml_content: &str,
        execution_date: chrono::NaiveDate,
    ) -> Self {
        let sql = version.get_sql_for_date(execution_date);
        Self::create(sql, &version.schema, yaml_content)
    }

    pub fn decompress_sql(&self) -> Option<String> {
        decompress_from_base64(&self.sql_compressed)
    }

    pub fn decompress_yaml(&self) -> Option<String> {
        decompress_from_base64(&self.yaml_compressed)
    }
}

pub fn compress_to_base64(content: &str) -> String {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(content.as_bytes()).ok();
    let compressed = encoder.finish().unwrap_or_default();
    BASE64.encode(&compressed)
}

pub fn decompress_from_base64(encoded: &str) -> Option<String> {
    let compressed = BASE64.decode(encoded).ok()?;
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).ok()?;
    Some(decompressed)
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

    #[test]
    fn test_compress_decompress_roundtrip() {
        let original = "SELECT * FROM table WHERE date = @partition_date";
        let compressed = compress_to_base64(original);
        let decompressed = decompress_from_base64(&compressed);

        assert!(decompressed.is_some());
        assert_eq!(decompressed.unwrap(), original);
    }

    #[test]
    fn test_compress_reduces_size_for_large_content() {
        let large_content = "SELECT ".to_string() + &"column, ".repeat(1000) + "FROM table";
        let compressed = compress_to_base64(&large_content);

        // Compressed + base64 should still be smaller for repetitive content
        assert!(compressed.len() < large_content.len());
    }

    #[test]
    fn test_execution_artifact_roundtrip() {
        let sql = "SELECT COUNT(*) FROM events WHERE date = @partition_date";
        let yaml = "name: test_query\nversion: 1\n";
        let schema = Schema::default();

        let artifact = ExecutionArtifact::create(sql, &schema, yaml);

        assert_eq!(artifact.decompress_sql().unwrap(), sql);
        assert_eq!(artifact.decompress_yaml().unwrap(), yaml);
        // Checksums are computed on compressed base64 content
        assert_eq!(artifact.sql_checksum, Checksums::sha256(&compress_to_base64(sql)));
        assert_eq!(artifact.yaml_checksum, Checksums::sha256(&compress_to_base64(yaml)));
    }
}
