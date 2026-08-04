pub(crate) struct ResourceMutationLog {
    kind: &'static str,
    action: &'static str,
    name: String,
    revision: Option<u64>,
}

impl ResourceMutationLog {
    pub(crate) fn new(
        kind: &'static str,
        action: &'static str,
        name: impl Into<String>,
        revision: Option<u64>,
    ) -> Self {
        Self {
            kind,
            action,
            name: name.into(),
            revision,
        }
    }

    pub(crate) fn set_revision(&mut self, revision: Option<u64>) {
        self.revision = revision;
    }

    pub(crate) fn log_success(&self) {
        let Some(revision) = self.revision else {
            return;
        };
        tracing::info!(
            kind = self.kind,
            name = %self.name,
            action = self.action,
            revision,
            "rest api audit"
        );
    }

    pub(crate) fn log_failure(&self, _error: &(impl std::fmt::Display + ?Sized)) {
        let Some(revision) = self.revision else {
            return;
        };
        tracing::error!(
            kind = self.kind,
            name = %self.name,
            action = self.action,
            revision,
            "rest api audit failed"
        );
    }
}
