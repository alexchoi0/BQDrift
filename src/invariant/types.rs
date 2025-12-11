use serde::{Deserialize, Serialize};

/// Raw invariants definition - can be inline, reference, or extended
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InvariantsRef {
    /// Reference to another version's invariants: ${{ versions.1.invariants }}
    Reference(String),
    /// Extended from base with add/modify/remove
    /// Note: Must come before Inline because InvariantsDef has defaults and would match anything
    Extended(ExtendedInvariants),
    /// Inline definition
    Inline(InvariantsDef),
}

impl Default for InvariantsRef {
    fn default() -> Self {
        InvariantsRef::Inline(InvariantsDef::default())
    }
}

/// Extended invariants - inherit from base and add/modify/remove
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedInvariants {
    /// Base invariants to inherit from: ${{ versions.1.invariants }}
    pub base: String,
    /// Invariants to add
    #[serde(default)]
    pub add: Option<InvariantsDef>,
    /// Invariants to modify (matched by name)
    #[serde(default)]
    pub modify: Option<InvariantsDef>,
    /// Invariants to remove (by name)
    #[serde(default)]
    pub remove: Option<InvariantsRemove>,
}

/// Names of invariants to remove, scoped by before/after
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InvariantsRemove {
    #[serde(default)]
    pub before: Vec<String>,
    #[serde(default)]
    pub after: Vec<String>,
}

/// Resolved invariants definition with before/after lists
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InvariantsDef {
    #[serde(default)]
    pub before: Vec<InvariantDef>,
    #[serde(default)]
    pub after: Vec<InvariantDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantDef {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub severity: Severity,
    #[serde(flatten)]
    pub check: InvariantCheck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InvariantCheck {
    /// Row count check - validates min/max row counts
    RowCount {
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        min: Option<i64>,
        #[serde(default)]
        max: Option<i64>,
    },

    /// Null percentage check - validates % of nulls in a column
    NullPercentage {
        #[serde(default)]
        source: Option<String>,
        column: String,
        max_percentage: f64,
    },

    /// Value range check - validates min/max values for a column
    ValueRange {
        #[serde(default)]
        source: Option<String>,
        column: String,
        #[serde(default)]
        min: Option<f64>,
        #[serde(default)]
        max: Option<f64>,
    },

    /// Distinct count check - validates cardinality of a column
    DistinctCount {
        #[serde(default)]
        source: Option<String>,
        column: String,
        #[serde(default)]
        min: Option<i64>,
        #[serde(default)]
        max: Option<i64>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    #[default]
    Error,
    Warning,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Error => write!(f, "error"),
            Severity::Warning => write!(f, "warning"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_row_count_no_source() {
        let yaml = r#"
name: row_count_check
type: row_count
min: 100
max: 1000000
severity: error
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        match inv.check {
            InvariantCheck::RowCount { source, min, max } => {
                assert!(source.is_none());
                assert_eq!(min, Some(100));
                assert_eq!(max, Some(1000000));
            }
            _ => panic!("Expected RowCount"),
        }
    }

    #[test]
    fn test_parse_row_count_with_source() {
        let yaml = r#"
name: custom_row_count
type: row_count
source: SELECT * FROM filtered_table
min: 50
severity: error
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        match inv.check {
            InvariantCheck::RowCount { source: Some(sql), min, .. } => {
                assert_eq!(sql, "SELECT * FROM filtered_table");
                assert_eq!(min, Some(50));
            }
            _ => panic!("Expected RowCount with source"),
        }
    }

    #[test]
    fn test_parse_null_percentage() {
        let yaml = r#"
name: null_check
type: null_percentage
column: user_count
max_percentage: 5.0
severity: warning
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        match inv.check {
            InvariantCheck::NullPercentage { source, column, max_percentage } => {
                assert!(source.is_none());
                assert_eq!(column, "user_count");
                assert!((max_percentage - 5.0).abs() < 0.001);
            }
            _ => panic!("Expected NullPercentage"),
        }
    }

    #[test]
    fn test_parse_value_range() {
        let yaml = r#"
name: revenue_bounds
type: value_range
column: revenue
min: 0.0
max: 1000000.0
severity: warning
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        match inv.check {
            InvariantCheck::ValueRange { column, min, max, .. } => {
                assert_eq!(column, "revenue");
                assert_eq!(min, Some(0.0));
                assert_eq!(max, Some(1000000.0));
            }
            _ => panic!("Expected ValueRange"),
        }
    }

    #[test]
    fn test_parse_distinct_count() {
        let yaml = r#"
name: region_cardinality
type: distinct_count
column: region
min: 1
max: 100
severity: warning
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        match inv.check {
            InvariantCheck::DistinctCount { column, min, max, .. } => {
                assert_eq!(column, "region");
                assert_eq!(min, Some(1));
                assert_eq!(max, Some(100));
            }
            _ => panic!("Expected DistinctCount"),
        }
    }

    #[test]
    fn test_parse_invariants_def() {
        let yaml = r#"
before:
  - name: source_check
    type: row_count
    source: SELECT 1 WHERE FALSE
    max: 0
    severity: error
after:
  - name: row_count
    type: row_count
    min: 100
    severity: error
  - name: null_check
    type: null_percentage
    column: user_id
    max_percentage: 1.0
    severity: warning
"#;
        let inv: InvariantsDef = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(inv.before.len(), 1);
        assert_eq!(inv.after.len(), 2);
    }

    #[test]
    fn test_default_severity_is_error() {
        let yaml = r#"
name: test
type: row_count
min: 1
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(inv.severity, Severity::Error);
    }

    #[test]
    fn test_parse_invariants_ref_inline() {
        let yaml = r#"
before:
  - name: check1
    type: row_count
    min: 100
    severity: error
after: []
"#;
        let inv_ref: InvariantsRef = serde_yaml::from_str(yaml).unwrap();
        match inv_ref {
            InvariantsRef::Inline(def) => {
                assert_eq!(def.before.len(), 1);
                assert_eq!(def.after.len(), 0);
            }
            _ => panic!("Expected Inline"),
        }
    }

    #[test]
    fn test_parse_invariants_ref_reference() {
        let yaml = r#""${{ versions.1.invariants }}""#;
        let inv_ref: InvariantsRef = serde_yaml::from_str(yaml).unwrap();
        match inv_ref {
            InvariantsRef::Reference(r) => {
                assert_eq!(r, "${{ versions.1.invariants }}");
            }
            _ => panic!("Expected Reference"),
        }
    }

    #[test]
    fn test_parse_invariants_ref_extended() {
        let yaml = r#"
base: "${{ versions.1.invariants }}"
add:
  after:
    - name: new_check
      type: row_count
      min: 200
      severity: error
modify:
  after:
    - name: row_count
      type: row_count
      min: 500
      severity: error
remove:
  after:
    - null_check
"#;
        let inv_ref: InvariantsRef = serde_yaml::from_str(yaml).unwrap();
        match inv_ref {
            InvariantsRef::Extended(ext) => {
                assert_eq!(ext.base, "${{ versions.1.invariants }}");
                assert!(ext.add.is_some());
                assert_eq!(ext.add.as_ref().unwrap().after.len(), 1);
                assert!(ext.modify.is_some());
                assert_eq!(ext.modify.as_ref().unwrap().after.len(), 1);
                assert!(ext.remove.is_some());
                assert_eq!(ext.remove.as_ref().unwrap().after.len(), 1);
                assert_eq!(ext.remove.as_ref().unwrap().after[0], "null_check");
            }
            _ => panic!("Expected Extended"),
        }
    }

    #[test]
    fn test_parse_extended_invariants_add_only() {
        let yaml = r#"
base: "${{ versions.1.invariants }}"
add:
  before:
    - name: pre_check
      type: row_count
      source: SELECT 1 WHERE FALSE
      max: 0
      severity: error
"#;
        let inv_ref: InvariantsRef = serde_yaml::from_str(yaml).unwrap();
        match inv_ref {
            InvariantsRef::Extended(ext) => {
                assert!(ext.add.is_some());
                assert_eq!(ext.add.as_ref().unwrap().before.len(), 1);
                assert!(ext.modify.is_none());
                assert!(ext.remove.is_none());
            }
            _ => panic!("Expected Extended"),
        }
    }

    #[test]
    fn test_parse_row_count_with_multiline_source() {
        let yaml = r#"
name: inline_check
type: row_count
source: |
  SELECT * FROM my_table WHERE status = 'active'
min: 10
severity: warning
"#;
        let inv: InvariantDef = serde_yaml::from_str(yaml).unwrap();
        match inv.check {
            InvariantCheck::RowCount { source: Some(sql), min, .. } => {
                assert!(sql.contains("SELECT * FROM my_table"));
                assert_eq!(min, Some(10));
            }
            _ => panic!("Expected RowCount with source"),
        }
    }
}
