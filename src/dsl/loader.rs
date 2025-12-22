use std::collections::HashMap;
use std::path::Path;
use crate::error::{BqDriftError, Result};
use crate::schema::{ClusterConfig, Schema};
use crate::invariant::InvariantsDef;
use super::parser::{
    QueryDef, VersionDef, ResolvedRevision, RawQueryDef,
};
use super::resolver::VariableResolver;
use super::dependencies::SqlDependencies;
use super::preprocessor::YamlPreprocessor;

pub use bq_runner::{FileLoader, SqlLoader, SqlFile};

pub struct QueryLoader {
    resolver: VariableResolver,
    preprocessor: YamlPreprocessor,
}

impl QueryLoader {
    pub fn new() -> Self {
        Self {
            resolver: VariableResolver::new(),
            preprocessor: YamlPreprocessor::new(),
        }
    }

    pub fn load_dir(&self, path: impl AsRef<Path>) -> Result<Vec<QueryDef>> {
        let yaml_files = FileLoader::load_dir(&path, "yaml")
            .map_err(|e| BqDriftError::DslParse(e.to_string()))?;

        yaml_files
            .into_iter()
            .map(|file| self.load_query(&file.path))
            .collect()
    }

    pub fn load_sql_dir(&self, path: impl AsRef<Path>) -> Result<Vec<SqlFile>> {
        SqlLoader::load_dir(path)
            .map_err(|e| BqDriftError::DslParse(e.to_string()))
    }

    pub fn load_sql_file(&self, path: impl AsRef<Path>) -> Result<SqlFile> {
        SqlLoader::load_file(path)
            .map_err(|e| BqDriftError::DslParse(e.to_string()))
    }

    pub fn load_yaml_contents(&self, path: impl AsRef<Path>) -> Result<HashMap<String, String>> {
        let yaml_files = FileLoader::load_dir(&path, "yaml")
            .map_err(|e| BqDriftError::DslParse(e.to_string()))?;

        let mut contents = HashMap::new();
        for file in yaml_files {
            let base_dir = file.path.parent().unwrap_or(Path::new("."));
            let processed = self.preprocessor.process(&file.content, base_dir)?;
            let raw: RawQueryDef = serde_yaml::from_str(&processed)?;
            contents.insert(raw.name, processed);
        }
        Ok(contents)
    }

    pub fn load_query(&self, yaml_path: impl AsRef<Path>) -> Result<QueryDef> {
        let yaml_path = yaml_path.as_ref();
        let file = FileLoader::load_file(yaml_path)
            .map_err(|e| BqDriftError::DslParse(e.to_string()))?;

        let base_dir = yaml_path.parent().unwrap_or(Path::new("."));
        let processed = self.preprocessor.process(&file.content, base_dir)?;

        let raw: RawQueryDef = serde_yaml::from_str(&processed)?;

        self.resolve_query(raw, base_dir)
    }

    fn resolve_query(&self, raw: RawQueryDef, _base_dir: &Path) -> Result<QueryDef> {
        let mut resolved_schemas: HashMap<u32, Schema> = HashMap::new();
        let mut resolved_invariants: HashMap<u32, InvariantsDef> = HashMap::new();
        let mut versions: Vec<VersionDef> = Vec::new();

        let mut sorted_versions = raw.versions.clone();
        sorted_versions.sort_by_key(|v| v.version);

        for raw_version in sorted_versions {
            let schema = self.resolver.resolve_schema(
                &raw_version.schema,
                &resolved_schemas,
            )?;

            let sql_content = raw_version.source.clone();
            let dependencies = SqlDependencies::extract(&sql_content).tables;

            let revisions = self.resolve_revisions(&raw_version.revisions)?;

            let invariants = self.resolver.resolve_invariants(
                &raw_version.invariants,
                &resolved_invariants,
            )?;

            resolved_schemas.insert(raw_version.version, schema.clone());
            resolved_invariants.insert(raw_version.version, invariants.clone());

            versions.push(VersionDef {
                version: raw_version.version,
                effective_from: raw_version.effective_from,
                source: "<inline>".to_string(),
                sql_content,
                revisions,
                description: raw_version.description,
                backfill_since: raw_version.backfill_since,
                schema,
                dependencies,
                invariants,
            });
        }

        let cluster = match &raw.destination.cluster {
            Some(fields) => Some(ClusterConfig::new(fields.clone())?),
            None => None,
        };

        Ok(QueryDef {
            name: raw.name,
            destination: raw.destination,
            description: raw.description,
            owner: raw.owner,
            tags: raw.tags,
            versions,
            cluster,
        })
    }

    fn resolve_revisions(
        &self,
        revisions: &[super::parser::Revision],
    ) -> Result<Vec<ResolvedRevision>> {
        revisions
            .iter()
            .map(|rev| {
                let sql_content = rev.source.clone();
                let dependencies = SqlDependencies::extract(&sql_content).tables;

                Ok(ResolvedRevision {
                    revision: rev.revision,
                    effective_from: rev.effective_from,
                    source: "<inline>".to_string(),
                    sql_content,
                    reason: rev.reason.clone(),
                    backfill_since: rev.backfill_since,
                    dependencies,
                })
            })
            .collect()
    }
}

impl Default for QueryLoader {
    fn default() -> Self {
        Self::new()
    }
}
