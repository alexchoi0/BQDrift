use serde::{Deserialize, Serialize};
use super::field::Field;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Schema {
    #[serde(default)]
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn add_field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    pub fn add_fields(mut self, fields: impl IntoIterator<Item = Field>) -> Self {
        self.fields.extend(fields);
        self
    }

    pub fn remove_field(mut self, name: &str) -> Self {
        self.fields.retain(|f| f.name != name);
        self
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }
}
