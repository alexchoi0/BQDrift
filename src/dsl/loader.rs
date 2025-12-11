use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use glob::glob;
use crate::error::{BqDriftError, Result};
use crate::schema::{ClusterConfig, Schema};
use crate::invariant::{InvariantsDef, InvariantDef, InvariantCheck, SqlSource};
use super::parser::{
    QueryDef, VersionDef, ResolvedRevision, RawQueryDef,
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

    pub fn load_yaml_contents(&self, path: impl AsRef<Path>) -> Result<HashMap<String, String>> {
        let pattern = path.as_ref().join("**/*.yaml");
        let pattern_str = pattern.to_string_lossy();

        let yaml_files: Vec<PathBuf> = glob(&pattern_str)
            .map_err(|e| BqDriftError::DslParse(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        let mut contents = HashMap::new();
        for yaml_path in yaml_files {
            let yaml_content = fs::read_to_string(&yaml_path)
                .map_err(|_| BqDriftError::YamlFileNotFound(yaml_path.display().to_string()))?;
            let raw: RawQueryDef = serde_yaml::from_str(&yaml_content)?;
            contents.insert(raw.name, yaml_content);
        }
        Ok(contents)
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
        let mut resolved_invariants: HashMap<u32, InvariantsDef> = HashMap::new();
        let mut versions: Vec<VersionDef> = Vec::new();

        let mut sorted_versions = raw.versions.clone();
        sorted_versions.sort_by_key(|v| v.version);

        for raw_version in sorted_versions {
            let schema = self.resolver.resolve_schema(
                &raw_version.schema,
                &resolved_schemas,
            )?;

            let (source_name, sql_content) = match &raw_version.source {
                SqlSource::Source(path) => {
                    let resolved_path = self.resolver.resolve_sql_ref(path, &resolved_sqls)?;
                    let sql_path = base_dir.join(&resolved_path);
                    let content = fs::read_to_string(&sql_path)
                        .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;
                    (resolved_path, content)
                }
                SqlSource::SourceInline(content) => {
                    ("<inline>".to_string(), content.clone())
                }
            };

            let dependencies = SqlDependencies::extract(&sql_content).tables;

            let revisions = self.resolve_revisions(
                &raw_version.revisions,
                base_dir,
            )?;

            let invariants = self.resolver.resolve_invariants(
                &raw_version.invariants,
                &resolved_invariants,
            )?;

            let invariants = self.load_invariant_sql_files(invariants, base_dir)?;

            resolved_schemas.insert(raw_version.version, schema.clone());
            if let SqlSource::Source(path) = &raw_version.source {
                resolved_sqls.insert(raw_version.version, path.clone());
            }
            resolved_invariants.insert(raw_version.version, invariants.clone());

            versions.push(VersionDef {
                version: raw_version.version,
                effective_from: raw_version.effective_from,
                source: source_name,
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
        base_dir: &Path,
    ) -> Result<Vec<ResolvedRevision>> {
        revisions
            .iter()
            .map(|rev| {
                let (source_name, sql_content) = match &rev.source {
                    SqlSource::Source(path) => {
                        let sql_path = base_dir.join(path);
                        let content = fs::read_to_string(&sql_path)
                            .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;
                        (path.clone(), content)
                    }
                    SqlSource::SourceInline(content) => {
                        ("<inline>".to_string(), content.clone())
                    }
                };

                let dependencies = SqlDependencies::extract(&sql_content).tables;

                Ok(ResolvedRevision {
                    revision: rev.revision,
                    effective_from: rev.effective_from,
                    source: source_name,
                    sql_content,
                    reason: rev.reason.clone(),
                    backfill_since: rev.backfill_since,
                    dependencies,
                })
            })
            .collect()
    }

    fn load_invariant_sql_files(
        &self,
        mut invariants: InvariantsDef,
        base_dir: &Path,
    ) -> Result<InvariantsDef> {
        for inv in &mut invariants.before {
            self.load_invariant_check_sql(inv, base_dir)?;
        }
        for inv in &mut invariants.after {
            self.load_invariant_check_sql(inv, base_dir)?;
        }
        Ok(invariants)
    }

    fn load_invariant_check_sql(
        &self,
        inv: &mut InvariantDef,
        base_dir: &Path,
    ) -> Result<()> {
        match &inv.check {
            InvariantCheck::RowCount { source, min, max } => {
                if let Some(SqlSource::Source(path)) = source {
                    let sql_path = base_dir.join(path);
                    let content = fs::read_to_string(&sql_path)
                        .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;
                    inv.check = InvariantCheck::RowCount {
                        source: Some(SqlSource::SourceInline(content)),
                        min: *min,
                        max: *max,
                    };
                }
            }
            InvariantCheck::NullPercentage { source, column, max_percentage } => {
                if let Some(SqlSource::Source(path)) = source {
                    let sql_path = base_dir.join(path);
                    let content = fs::read_to_string(&sql_path)
                        .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;
                    inv.check = InvariantCheck::NullPercentage {
                        source: Some(SqlSource::SourceInline(content)),
                        column: column.clone(),
                        max_percentage: *max_percentage,
                    };
                }
            }
            InvariantCheck::ValueRange { source, column, min, max } => {
                if let Some(SqlSource::Source(path)) = source {
                    let sql_path = base_dir.join(path);
                    let content = fs::read_to_string(&sql_path)
                        .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;
                    inv.check = InvariantCheck::ValueRange {
                        source: Some(SqlSource::SourceInline(content)),
                        column: column.clone(),
                        min: *min,
                        max: *max,
                    };
                }
            }
            InvariantCheck::DistinctCount { source, column, min, max } => {
                if let Some(SqlSource::Source(path)) = source {
                    let sql_path = base_dir.join(path);
                    let content = fs::read_to_string(&sql_path)
                        .map_err(|_| BqDriftError::SqlFileNotFound(sql_path.display().to_string()))?;
                    inv.check = InvariantCheck::DistinctCount {
                        source: Some(SqlSource::SourceInline(content)),
                        column: column.clone(),
                        min: *min,
                        max: *max,
                    };
                }
            }
        }
        Ok(())
    }
}

impl Default for QueryLoader {
    fn default() -> Self {
        Self::new()
    }
}
