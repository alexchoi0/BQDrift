use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    FunctionArg, FunctionArgExpr,
};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct SqlDependencies {
    pub tables: HashSet<String>,
}

impl SqlDependencies {
    pub fn extract(sql: &str) -> Self {
        let dialect = BigQueryDialect {};
        let mut deps = SqlDependencies::default();

        match Parser::parse_sql(&dialect, sql) {
            Ok(statements) => {
                for statement in statements {
                    deps.extract_from_statement(&statement);
                }
            }
            Err(_) => {
                // If parsing fails, try to extract tables using regex fallback
                deps.extract_fallback(sql);
            }
        }

        deps
    }

    fn extract_from_statement(&mut self, statement: &Statement) {
        match statement {
            Statement::Query(query) => {
                self.extract_from_query(query);
            }
            Statement::Insert(insert) => {
                // Extract source from INSERT ... SELECT
                if let Some(source) = &insert.source {
                    self.extract_from_query(source);
                }
            }
            Statement::CreateTable(create) => {
                // Extract from CREATE TABLE ... AS SELECT
                if let Some(query) = &create.query {
                    self.extract_from_query(query);
                }
            }
            Statement::CreateView { query, .. } => {
                self.extract_from_query(query);
            }
            Statement::Merge { source, .. } => {
                self.extract_from_table_factor(source);
            }
            _ => {}
        }
    }

    fn extract_from_query(&mut self, query: &Query) {
        // Handle CTEs (WITH clause)
        let cte_names: HashSet<String> = query
            .with
            .as_ref()
            .map(|w| {
                w.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.clone())
                    .collect()
            })
            .unwrap_or_default();

        // Extract from CTEs themselves
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.extract_from_query(&cte.query);
            }
        }

        // Extract from main query body
        self.extract_from_set_expr(&query.body, &cte_names);
    }

    fn extract_from_set_expr(&mut self, set_expr: &SetExpr, cte_names: &HashSet<String>) {
        match set_expr {
            SetExpr::Select(select) => {
                self.extract_from_select(select, cte_names);
            }
            SetExpr::Query(query) => {
                self.extract_from_query(query);
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.extract_from_set_expr(left, cte_names);
                self.extract_from_set_expr(right, cte_names);
            }
            SetExpr::Values(_) => {}
            SetExpr::Insert(_) => {}
            SetExpr::Update(_) => {}
            SetExpr::Table(table) => {
                let name = table.table_name.as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                if !name.is_empty() && !cte_names.contains(&name) {
                    self.tables.insert(name);
                }
            }
        }
    }

    fn extract_from_select(&mut self, select: &Select, cte_names: &HashSet<String>) {
        // Extract from FROM clause
        for table_with_joins in &select.from {
            self.extract_from_table_with_joins(table_with_joins, cte_names);
        }

        // Extract from subqueries in SELECT items
        for item in &select.projection {
            if let SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) = item {
                self.extract_from_expr(expr, cte_names);
            }
        }

        // Extract from WHERE clause
        if let Some(selection) = &select.selection {
            self.extract_from_expr(selection, cte_names);
        }

        // Extract from HAVING clause
        if let Some(having) = &select.having {
            self.extract_from_expr(having, cte_names);
        }
    }

    fn extract_from_table_with_joins(
        &mut self,
        table_with_joins: &TableWithJoins,
        cte_names: &HashSet<String>,
    ) {
        self.extract_from_table_factor_with_ctes(&table_with_joins.relation, cte_names);

        for join in &table_with_joins.joins {
            self.extract_from_table_factor_with_ctes(&join.relation, cte_names);
        }
    }

    fn extract_from_table_factor(&mut self, table_factor: &TableFactor) {
        self.extract_from_table_factor_with_ctes(table_factor, &HashSet::new());
    }

    fn extract_from_table_factor_with_ctes(
        &mut self,
        table_factor: &TableFactor,
        cte_names: &HashSet<String>,
    ) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                // Don't add CTEs as dependencies
                if !cte_names.contains(&table_name) {
                    self.tables.insert(table_name);
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.extract_from_query(subquery);
            }
            TableFactor::TableFunction { .. } => {}
            TableFactor::UNNEST { .. } => {}
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.extract_from_table_with_joins(table_with_joins, cte_names);
            }
            TableFactor::Pivot { table, .. } => {
                self.extract_from_table_factor_with_ctes(table, cte_names);
            }
            TableFactor::Unpivot { table, .. } => {
                self.extract_from_table_factor_with_ctes(table, cte_names);
            }
            TableFactor::Function { .. } => {}
            TableFactor::JsonTable { .. } => {}
            TableFactor::MatchRecognize { table, .. } => {
                self.extract_from_table_factor_with_ctes(table, cte_names);
            }
            _ => {}
        }
    }

    fn extract_from_expr(&mut self, expr: &Expr, cte_names: &HashSet<String>) {
        match expr {
            Expr::Subquery(query) => {
                self.extract_from_query(query);
            }
            Expr::InSubquery { subquery, .. } => {
                self.extract_from_query(subquery);
            }
            Expr::Exists { subquery, .. } => {
                self.extract_from_query(subquery);
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_from_expr(left, cte_names);
                self.extract_from_expr(right, cte_names);
            }
            Expr::UnaryOp { expr, .. } => {
                self.extract_from_expr(expr, cte_names);
            }
            Expr::Between { expr, low, high, .. } => {
                self.extract_from_expr(expr, cte_names);
                self.extract_from_expr(low, cte_names);
                self.extract_from_expr(high, cte_names);
            }
            Expr::Case { operand, conditions, results, else_result, .. } => {
                if let Some(op) = operand {
                    self.extract_from_expr(op, cte_names);
                }
                for cond in conditions {
                    self.extract_from_expr(cond, cte_names);
                }
                for result in results {
                    self.extract_from_expr(result, cte_names);
                }
                if let Some(else_r) = else_result {
                    self.extract_from_expr(else_r, cte_names);
                }
            }
            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.extract_from_expr(e, cte_names);
                        }
                    }
                }
            }
            Expr::Nested(nested) => {
                self.extract_from_expr(nested, cte_names);
            }
            _ => {}
        }
    }

    fn extract_fallback(&mut self, sql: &str) {
        // Fallback regex-based extraction for simple cases
        let sql_upper = sql.to_uppercase();
        let re = regex::Regex::new(
            r"(?i)\b(?:FROM|JOIN|INTO|UPDATE|MERGE\s+INTO)\s+`?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`?",
        )
        .unwrap();

        for cap in re.captures_iter(&sql_upper) {
            if let Some(table) = cap.get(1) {
                let table_name = table.as_str().to_string();
                // Skip common keywords that might be captured
                if !["SELECT", "WHERE", "AND", "OR", "ON", "AS", "SET"].contains(&table_name.as_str())
                {
                    self.tables.insert(table_name.to_lowercase());
                }
            }
        }
    }

    pub fn has_dependency(&self, table: &str) -> bool {
        self.tables.contains(table) || self.tables.iter().any(|t| t.ends_with(&format!(".{}", table)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sql = "SELECT * FROM my_table WHERE id = 1";
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("my_table"));
    }

    #[test]
    fn test_qualified_table_name() {
        let sql = "SELECT * FROM project.dataset.my_table";
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("project.dataset.my_table"));
    }

    #[test]
    fn test_join() {
        let sql = "SELECT a.*, b.name FROM table_a a JOIN table_b b ON a.id = b.id";
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("table_a"));
        assert!(deps.tables.contains("table_b"));
    }

    #[test]
    fn test_multiple_joins() {
        let sql = r#"
            SELECT *
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            LEFT JOIN products p ON o.product_id = p.id
        "#;
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("orders"));
        assert!(deps.tables.contains("customers"));
        assert!(deps.tables.contains("products"));
    }

    #[test]
    fn test_subquery() {
        let sql = r#"
            SELECT *
            FROM (SELECT * FROM inner_table) sub
            WHERE id IN (SELECT id FROM another_table)
        "#;
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("inner_table"));
        assert!(deps.tables.contains("another_table"));
    }

    #[test]
    fn test_cte() {
        let sql = r#"
            WITH cte AS (
                SELECT * FROM source_table
            )
            SELECT * FROM cte
        "#;
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("source_table"));
        // CTE name should NOT be in dependencies
        assert!(!deps.tables.contains("cte"));
    }

    #[test]
    fn test_multiple_ctes() {
        let sql = r#"
            WITH
                cte1 AS (SELECT * FROM table1),
                cte2 AS (SELECT * FROM table2)
            SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id
        "#;
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("table1"));
        assert!(deps.tables.contains("table2"));
        assert!(!deps.tables.contains("cte1"));
        assert!(!deps.tables.contains("cte2"));
    }

    #[test]
    fn test_union() {
        let sql = r#"
            SELECT * FROM table1
            UNION ALL
            SELECT * FROM table2
        "#;
        let deps = SqlDependencies::extract(sql);
        assert!(deps.tables.contains("table1"));
        assert!(deps.tables.contains("table2"));
    }

    #[test]
    fn test_bigquery_backticks() {
        let sql = "SELECT * FROM `project.dataset.table`";
        let deps = SqlDependencies::extract(sql);
        // BigQuery dialect parses backtick-quoted identifiers
        assert!(deps.tables.iter().any(|t| t.contains("project") && t.contains("dataset") && t.contains("table")));
    }

    #[test]
    fn test_has_dependency() {
        let sql = "SELECT * FROM analytics.daily_stats";
        let deps = SqlDependencies::extract(sql);
        assert!(deps.has_dependency("analytics.daily_stats"));
        assert!(deps.has_dependency("daily_stats"));
    }
}
