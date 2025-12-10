use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use glob::glob;
use crate::error::{BqDriftError, Result};
use crate::schema::{ClusterConfig, Schema};
use super::parser::{
    QueryDef, VersionDef, ResolvedSqlRevision, RawQueryDef,
};
use super::resolver::VariableResolver;
use super::dependencies::SqlDependencies;

pub struct QueryLoader {
    resolver: VariableResolver,
}

impl QueryLoader {
    pub fn new() -> Self {
        Self {
            resolver: VariableResolver::new(),
        }
    }

    pub fn load_dir(&self, path: impl AsRef<Path>) -> Result<Vec<QueryDef>> {
        let pattern = path.as_ref().join("**/*.yaml");
        let pattern_str = pattern.to_string_lossy();

        let yaml_files: Vec<PathBuf> = glob(&pattern_str)
            .map_err(|e| BqDriftError::DslParse(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        yaml_files
            .into_iter()
            .map(|yaml_path| self.load_query(&yaml_path))
            .collect()
    }

    pub fn load_query(&self, yaml_path: impl AsRef<Path>) -> Result<QueryDef> {
        let yaml_path = yaml_path.as_ref();
        let yaml_content = fs::read_to_string(yaml_path)
            .map_err(|_| BqDriftError::YamlFileNotFound(yaml_path.display().to_string()))?;

        let raw: RawQueryDef = serde_yaml::from_str(&yaml_content)?;
        let base_dir = yaml_path.parent().unwrap_or(Path::new("."));

        self.resolve_query(raw, base_dir)
    }

    fn resolve_query(&self, raw: RawQueryDef, base_dir: &Path) -> Result<QueryDef> {
        let mut resolved_schemas: HashMap<u32, Schema> = HashMap::new();
        let mut resolved_sqls: HashMap<u32, String> = HashMap::new();
        let mut versions: Vec<VersionDef> = Vec::new();

        let mut sorted_versions = raw.versions.clone();
        sorted_versions.sort_by_key(|v| v.version);

        for raw_version in sorted_versions {
            let schema = self.resolver.resolve_schema(
                &raw_version.schema,
                &resolved_schemas,
            )?;

            let sql_filename = self.resolver.resolve_sql_ref(
                &raw_version.sql,
                &resolved_sqls,
            )?;

            let sql_path = base_dir.join(&sql_filename);
            let sql_content = fs::read_to_string(&sql_path)
                .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;

            let dependencies = SqlDependencies::extract(&sql_content).tables;

            let sql_revisions = self.resolve_sql_revisions(
                &raw_version.sql_revisions,
                base_dir,
            )?;

            resolved_schemas.insert(raw_version.version, schema.clone());
            resolved_sqls.insert(raw_version.version, sql_filename.clone());

            versions.push(VersionDef {
                version: raw_version.version,
                effective_from: raw_version.effective_from,
                sql: sql_filename,
                sql_content,
                sql_revisions,
                description: raw_version.description,
                backfill_since: raw_version.backfill_since,
                schema,
                dependencies,
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

    fn resolve_sql_revisions(
        &self,
        revisions: &[super::parser::SqlRevision],
        base_dir: &Path,
    ) -> Result<Vec<ResolvedSqlRevision>> {
        revisions
            .iter()
            .map(|rev| {
                let sql_path = base_dir.join(&rev.sql);
                let sql_content = fs::read_to_string(&sql_path)
                    .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;

                let dependencies = SqlDependencies::extract(&sql_content).tables;

                Ok(ResolvedSqlRevision {
                    revision: rev.revision,
                    effective_from: rev.effective_from,
                    sql: rev.sql.clone(),
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
