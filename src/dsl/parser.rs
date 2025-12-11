use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use crate::schema::{Field, PartitionConfig, ClusterConfig, Schema};
use crate::invariant::{InvariantsRef, InvariantsDef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawQueryDef {
    pub name: String,
    pub destination: Destination,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub versions: Vec<RawVersionDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawVersionDef {
    pub version: u32,
    pub effective_from: NaiveDate,
    pub source: String,
    #[serde(default)]
    pub revisions: Vec<Revision>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub backfill_since: Option<NaiveDate>,
    pub schema: SchemaRef,
    #[serde(default)]
    pub invariants: Option<InvariantsRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SchemaRef {
    Inline(Vec<Field>),
    Reference(String),
    Extended(ExtendedSchema),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedSchema {
    pub base: String,
    #[serde(default)]
    pub add: Vec<Field>,
    #[serde(default)]
    pub modify: Vec<Field>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Revision {
    pub revision: u32,
    pub effective_from: NaiveDate,
    pub source: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub backfill_since: Option<NaiveDate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Destination {
    pub dataset: String,
    pub table: String,
    pub partition: PartitionConfig,
    #[serde(default)]
    pub cluster: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct QueryDef {
    pub name: String,
    pub destination: Destination,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub tags: Vec<String>,
    pub versions: Vec<VersionDef>,
    pub cluster: Option<ClusterConfig>,
}

#[derive(Debug, Clone)]
pub struct VersionDef {
    pub version: u32,
    pub effective_from: NaiveDate,
    pub source: String,
    pub sql_content: String,
    pub revisions: Vec<ResolvedRevision>,
    pub description: Option<String>,
    pub backfill_since: Option<NaiveDate>,
    pub schema: Schema,
    pub dependencies: HashSet<String>,
    pub invariants: InvariantsDef,
}

#[derive(Debug, Clone)]
pub struct ResolvedRevision {
    pub revision: u32,
    pub effective_from: NaiveDate,
    pub source: String,
    pub sql_content: String,
    pub reason: Option<String>,
    pub backfill_since: Option<NaiveDate>,
    pub dependencies: HashSet<String>,
}

impl VersionDef {
    pub fn get_sql_for_date(&self, execution_date: NaiveDate) -> &str {
        let applicable_revision = self.revisions
            .iter()
            .filter(|r| r.effective_from <= execution_date)
            .max_by_key(|r| r.effective_from);

        match applicable_revision {
            Some(rev) => &rev.sql_content,
            None => &self.sql_content,
        }
    }
}

impl QueryDef {
    pub fn get_version_for_date(&self, partition_date: NaiveDate) -> Option<&VersionDef> {
        self.versions
            .iter()
            .filter(|v| v.effective_from <= partition_date)
            .max_by_key(|v| v.effective_from)
    }

    pub fn latest_version(&self) -> Option<&VersionDef> {
        self.versions.iter().max_by_key(|v| v.version)
    }
}
