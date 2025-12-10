use super::types::Severity;

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub name: String,
    pub status: CheckStatus,
    pub severity: Severity,
    pub message: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Passed,
    Failed,
    Skipped,
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckStatus::Passed => write!(f, "passed"),
            CheckStatus::Failed => write!(f, "failed"),
            CheckStatus::Skipped => write!(f, "skipped"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InvariantReport {
    pub before: Vec<CheckResult>,
    pub after: Vec<CheckResult>,
}

impl InvariantReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_errors(&self) -> bool {
        self.before.iter().chain(self.after.iter()).any(|r| {
            r.status == CheckStatus::Failed && r.severity == Severity::Error
        })
    }

    pub fn has_before_errors(&self) -> bool {
        self.before.iter().any(|r| {
            r.status == CheckStatus::Failed && r.severity == Severity::Error
        })
    }

    pub fn has_after_errors(&self) -> bool {
        self.after.iter().any(|r| {
            r.status == CheckStatus::Failed && r.severity == Severity::Error
        })
    }

    pub fn has_warnings(&self) -> bool {
        self.before.iter().chain(self.after.iter()).any(|r| {
            r.status == CheckStatus::Failed && r.severity == Severity::Warning
        })
    }

    pub fn all_passed(&self) -> bool {
        self.before.iter().chain(self.after.iter()).all(|r| {
            r.status == CheckStatus::Passed
        })
    }

    pub fn passed_count(&self) -> usize {
        self.before.iter().chain(self.after.iter())
            .filter(|r| r.status == CheckStatus::Passed)
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.before.iter().chain(self.after.iter())
            .filter(|r| r.status == CheckStatus::Failed)
            .count()
    }

    pub fn skipped_count(&self) -> usize {
        self.before.iter().chain(self.after.iter())
            .filter(|r| r.status == CheckStatus::Skipped)
            .count()
    }
}

impl CheckResult {
    pub fn passed(name: impl Into<String>, severity: Severity, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Passed,
            severity,
            message: message.into(),
            details: None,
        }
    }

    pub fn failed(name: impl Into<String>, severity: Severity, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Failed,
            severity,
            message: message.into(),
            details: None,
        }
    }

    pub fn skipped(name: impl Into<String>, severity: Severity, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Skipped,
            severity,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub fn is_blocking_error(&self) -> bool {
        self.status == CheckStatus::Failed && self.severity == Severity::Error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_result_passed() {
        let result = CheckResult::passed("test", Severity::Error, "All good");
        assert_eq!(result.status, CheckStatus::Passed);
        assert!(!result.is_blocking_error());
    }

    #[test]
    fn test_check_result_failed_error() {
        let result = CheckResult::failed("test", Severity::Error, "Failed");
        assert_eq!(result.status, CheckStatus::Failed);
        assert!(result.is_blocking_error());
    }

    #[test]
    fn test_check_result_failed_warning() {
        let result = CheckResult::failed("test", Severity::Warning, "Warning");
        assert_eq!(result.status, CheckStatus::Failed);
        assert!(!result.is_blocking_error());
    }

    #[test]
    fn test_check_result_with_details() {
        let result = CheckResult::failed("test", Severity::Error, "Failed")
            .with_details("Row count was 50, expected >= 100");
        assert_eq!(result.details, Some("Row count was 50, expected >= 100".to_string()));
    }

    #[test]
    fn test_invariant_report_empty() {
        let report = InvariantReport::new();
        assert!(!report.has_errors());
        assert!(!report.has_warnings());
        assert!(report.all_passed());
        assert_eq!(report.passed_count(), 0);
    }

    #[test]
    fn test_invariant_report_with_results() {
        let mut report = InvariantReport::new();
        report.before.push(CheckResult::passed("check1", Severity::Error, "OK"));
        report.after.push(CheckResult::failed("check2", Severity::Warning, "Warn"));
        report.after.push(CheckResult::passed("check3", Severity::Error, "OK"));

        assert!(!report.has_errors());
        assert!(report.has_warnings());
        assert!(!report.all_passed());
        assert_eq!(report.passed_count(), 2);
        assert_eq!(report.failed_count(), 1);
    }

    #[test]
    fn test_invariant_report_has_before_errors() {
        let mut report = InvariantReport::new();
        report.before.push(CheckResult::failed("check1", Severity::Error, "Failed"));

        assert!(report.has_errors());
        assert!(report.has_before_errors());
        assert!(!report.has_after_errors());
    }

    #[test]
    fn test_invariant_report_has_after_errors() {
        let mut report = InvariantReport::new();
        report.after.push(CheckResult::failed("check1", Severity::Error, "Failed"));

        assert!(report.has_errors());
        assert!(!report.has_before_errors());
        assert!(report.has_after_errors());
    }
}
