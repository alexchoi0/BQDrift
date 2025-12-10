use serde::{Deserialize, Serialize};
use crate::error::{BqDriftError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterConfig {
    pub fields: Vec<String>,
}

impl ClusterConfig {
    pub fn new(fields: Vec<String>) -> Result<Self> {
        if fields.len() > 4 {
            return Err(BqDriftError::Cluster(
                "BigQuery supports maximum 4 clustering fields".into()
            ));
        }
        Ok(Self { fields })
    }

    pub fn from_fields(fields: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        let fields: Vec<String> = fields.into_iter().map(|f| f.into()).collect();
        Self::new(fields)
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }
}
