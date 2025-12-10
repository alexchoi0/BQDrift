use regex::Regex;
use std::collections::HashMap;
use crate::error::{BqDriftError, Result};
use crate::schema::{Field, Schema};
use crate::invariant::{
    InvariantsRef, InvariantsDef, ExtendedInvariants, InvariantDef,
};
use super::parser::{SchemaRef, ExtendedSchema};

pub struct VariableResolver {
    variable_pattern: Regex,
}

impl VariableResolver {
    pub fn new() -> Self {
        Self {
            variable_pattern: Regex::new(r"\$\{\{\s*versions\.(\d+)\.(\w+)\s*\}\}").unwrap(),
        }
    }

    pub fn resolve_schema(
        &self,
        schema_ref: &SchemaRef,
        resolved_versions: &HashMap<u32, Schema>,
    ) -> Result<Schema> {
        match schema_ref {
            SchemaRef::Inline(fields) => Ok(Schema::from_fields(fields.clone())),

            SchemaRef::Reference(ref_str) => {
                let version = self.extract_version_ref(ref_str)?;
                resolved_versions
                    .get(&version)
                    .cloned()
                    .ok_or_else(|| BqDriftError::InvalidVersionRef(
                        format!("Version {} not found or not yet resolved", version)
                    ))
            }

            SchemaRef::Extended(ext) => {
                self.resolve_extended_schema(ext, resolved_versions)
            }
        }
    }

    fn resolve_extended_schema(
        &self,
        ext: &ExtendedSchema,
        resolved_versions: &HashMap<u32, Schema>,
    ) -> Result<Schema> {
        let base_version = self.extract_version_ref(&ext.base)?;
        let base_schema = resolved_versions
            .get(&base_version)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(
                format!("Base version {} not found", base_version)
            ))?;

        let mut fields: Vec<Field> = base_schema.fields.clone();

        // Remove fields
        for name in &ext.remove {
            fields.retain(|f| &f.name != name);
        }

        // Modify existing fields (replace by name)
        for modified in &ext.modify {
            if let Some(field) = fields.iter_mut().find(|f| f.name == modified.name) {
                *field = modified.clone();
            }
        }

        // Add new fields
        fields.extend(ext.add.clone());

        Ok(Schema::from_fields(fields))
    }

    fn extract_version_ref(&self, ref_str: &str) -> Result<u32> {
        if let Some(caps) = self.variable_pattern.captures(ref_str) {
            let version: u32 = caps.get(1)
                .unwrap()
                .as_str()
                .parse()
                .map_err(|_| BqDriftError::InvalidVersionRef(ref_str.to_string()))?;
            Ok(version)
        } else {
            Err(BqDriftError::InvalidVersionRef(ref_str.to_string()))
        }
    }

    pub fn resolve_sql_ref(
        &self,
        sql_ref: &str,
        resolved_sqls: &HashMap<u32, String>,
    ) -> Result<String> {
        if let Some(caps) = self.variable_pattern.captures(sql_ref) {
            let version: u32 = caps.get(1)
                .unwrap()
                .as_str()
                .parse()
                .map_err(|_| BqDriftError::InvalidVersionRef(sql_ref.to_string()))?;

            let field = caps.get(2).unwrap().as_str();
            if field != "sql" {
                return Err(BqDriftError::VariableResolution(
                    format!("Expected 'sql' field, got '{}'", field)
                ));
            }

            resolved_sqls
                .get(&version)
                .cloned()
                .ok_or_else(|| BqDriftError::InvalidVersionRef(
                    format!("SQL for version {} not found", version)
                ))
        } else {
            Ok(sql_ref.to_string())
        }
    }

    pub fn is_variable_ref(&self, s: &str) -> bool {
        self.variable_pattern.is_match(s)
    }

    pub fn resolve_invariants(
        &self,
        inv_ref: &Option<InvariantsRef>,
        resolved_versions: &HashMap<u32, InvariantsDef>,
    ) -> Result<InvariantsDef> {
        match inv_ref {
            None => Ok(InvariantsDef::default()),

            Some(InvariantsRef::Inline(def)) => Ok(def.clone()),

            Some(InvariantsRef::Reference(ref_str)) => {
                let version = self.extract_invariants_version_ref(ref_str)?;
                resolved_versions
                    .get(&version)
                    .cloned()
                    .ok_or_else(|| BqDriftError::InvalidVersionRef(
                        format!("Invariants for version {} not found or not yet resolved", version)
                    ))
            }

            Some(InvariantsRef::Extended(ext)) => {
                self.resolve_extended_invariants(ext, resolved_versions)
            }
        }
    }

    fn resolve_extended_invariants(
        &self,
        ext: &ExtendedInvariants,
        resolved_versions: &HashMap<u32, InvariantsDef>,
    ) -> Result<InvariantsDef> {
        let base_version = self.extract_invariants_version_ref(&ext.base)?;
        let base = resolved_versions
            .get(&base_version)
            .ok_or_else(|| BqDriftError::InvalidVersionRef(
                format!("Base invariants version {} not found", base_version)
            ))?;

        let mut before: Vec<InvariantDef> = base.before.clone();
        let mut after: Vec<InvariantDef> = base.after.clone();

        if let Some(remove) = &ext.remove {
            before.retain(|inv| !remove.before.contains(&inv.name));
            after.retain(|inv| !remove.after.contains(&inv.name));
        }

        if let Some(modify) = &ext.modify {
            for modified in &modify.before {
                if let Some(inv) = before.iter_mut().find(|i| i.name == modified.name) {
                    *inv = modified.clone();
                }
            }
            for modified in &modify.after {
                if let Some(inv) = after.iter_mut().find(|i| i.name == modified.name) {
                    *inv = modified.clone();
                }
            }
        }

        if let Some(add) = &ext.add {
            before.extend(add.before.clone());
            after.extend(add.after.clone());
        }

        Ok(InvariantsDef { before, after })
    }

    fn extract_invariants_version_ref(&self, ref_str: &str) -> Result<u32> {
        if let Some(caps) = self.variable_pattern.captures(ref_str) {
            let version: u32 = caps.get(1)
                .unwrap()
                .as_str()
                .parse()
                .map_err(|_| BqDriftError::InvalidVersionRef(ref_str.to_string()))?;

            let field = caps.get(2).unwrap().as_str();
            if field != "invariants" {
                return Err(BqDriftError::VariableResolution(
                    format!("Expected 'invariants' field, got '{}'", field)
                ));
            }

            Ok(version)
        } else {
            Err(BqDriftError::InvalidVersionRef(ref_str.to_string()))
        }
    }
}

impl Default for VariableResolver {
    fn default() -> Self {
        Self::new()
    }
}
