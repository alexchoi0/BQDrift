use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum BqType {
    String,
    Bytes,
    Int64,
    Float64,
    Numeric,
    Bignumeric,
    Bool,
    Date,
    Datetime,
    Time,
    Timestamp,
    Geography,
    Json,
    Record,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum FieldMode {
    #[default]
    Nullable,
    Required,
    Repeated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: BqType,
    #[serde(default)]
    pub mode: FieldMode,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub fields: Option<Vec<Field>>,
}

impl Field {
    pub fn new(name: impl Into<String>, field_type: BqType) -> Self {
        Self {
            name: name.into(),
            field_type,
            mode: FieldMode::default(),
            nullable: true,
            description: None,
            fields: None,
        }
    }

    pub fn required(mut self) -> Self {
        self.mode = FieldMode::Required;
        self.nullable = false;
        self
    }

    pub fn repeated(mut self) -> Self {
        self.mode = FieldMode::Repeated;
        self
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn with_fields(mut self, fields: Vec<Field>) -> Self {
        self.fields = Some(fields);
        self
    }
}
