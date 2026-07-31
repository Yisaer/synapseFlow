use crate::FlowInstanceSpec;
use crate::instances::{
    DEFAULT_FLOW_INSTANCE_ID, FlowInstances, build_in_process_flow_instance,
    find_default_flow_instance_spec,
};
use crate::startup::StartupPhase;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage::StorageManager;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};

const STREAM_SHARED_REF_PERMITS: u32 = 1024;

/// Default patrol interval for the pipeline scheduler, in seconds.
pub const DEFAULT_PATROL_INTERVAL_SECS: u64 = 15;

#[derive(Clone)]
pub struct AppState {
    pub instances: FlowInstances,
    pub storage: Arc<StorageManager>,
    pub declared_instances: Arc<HashMap<String, ()>>,
    init_dir: Option<Arc<PathBuf>>,
    storage_operation_lock: Arc<Semaphore>,
    pipeline_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    stream_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    shared_mqtt_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl AppState {
    #[cfg(test)]
    pub fn new(
        instance: flow::FlowInstance,
        storage: StorageManager,
        flow_instances: Vec<FlowInstanceSpec>,
        patrol_interval_secs: u64,
    ) -> Result<Self, String> {
        Self::new_with_init_dir(
            instance,
            storage,
            flow_instances,
            patrol_interval_secs,
            None,
        )
    }

    pub(crate) fn new_with_init_dir(
        instance: flow::FlowInstance,
        storage: StorageManager,
        flow_instances: Vec<FlowInstanceSpec>,
        patrol_interval_secs: u64,
        init_dir: Option<PathBuf>,
    ) -> Result<Self, String> {
        let instances = FlowInstances::new(instance);
        let storage = Arc::new(storage);
        let mut declared_instances = HashMap::new();
        let state = Self {
            instances,
            storage,
            declared_instances: Arc::new(HashMap::new()),
            init_dir: init_dir.map(Arc::new),
            storage_operation_lock: Arc::new(Semaphore::new(1)),
            pipeline_op_locks: Arc::new(Mutex::new(HashMap::new())),
            stream_op_locks: Arc::new(Mutex::new(HashMap::new())),
            shared_mqtt_op_locks: Arc::new(Mutex::new(HashMap::new())),
        };

        find_default_flow_instance_spec(&flow_instances)?;

        for spec in &flow_instances {
            // `find_default_flow_instance_spec` already validated every id against
            // the resource-id grammar; the duplicate check uses the canonical id.
            let id = spec.id.as_str();
            if declared_instances.insert(id.to_string(), ()).is_some() {
                return Err(format!("duplicate flow instance id in config: {id}"));
            }
        }

        let default_instance = state.instances.default_instance();
        let shared_registries = default_instance.shared_registries();
        // Non-default instances are freshly built with an empty secret context;
        // propagate the bootstrapped store/policy from the default instance so
        // `store:NAME` references resolve everywhere (VF-51).
        let secret_ctx = default_instance.secret_context();
        let property_ctx = default_instance.property_context();
        for spec in &flow_instances {
            let id = spec.id.trim();
            if id == DEFAULT_FLOW_INSTANCE_ID {
                continue;
            }

            let instance = build_in_process_flow_instance(spec, Some(shared_registries.clone()))?;
            instance.set_secret_context(secret_ctx.clone());
            instance.set_property_context(property_ctx.clone());
            if state.instances.insert_local_instance(instance).is_some() {
                return Err(format!("duplicate flow instance id in runtime: {id}"));
            }
        }

        let app_state = Self {
            declared_instances: Arc::new(declared_instances),
            ..state
        };

        // Spawn the pipeline patrol scheduler.
        if patrol_interval_secs > 0 {
            app_state.spawn_scheduler(patrol_interval_secs);
        }

        Ok(app_state)
    }

    fn spawn_scheduler(&self, interval_secs: u64) {
        let storage = Arc::clone(&self.storage);
        let instances = self.instances.clone();
        tokio::spawn(async move {
            super::scheduler::run_patrol(storage, instances, Duration::from_secs(interval_secs))
                .await;
        });
    }

    pub fn is_declared_instance(&self, id: &str) -> bool {
        self.declared_instances.contains_key(id)
    }

    pub fn local_instance(&self, id: &str) -> Option<Arc<flow::FlowInstance>> {
        self.instances.get(id)
    }

    pub async fn bootstrap_from_storage(&self) -> Result<(), String> {
        crate::init_process::apply_init_directory_if_needed(
            self.storage.as_ref(),
            self.init_dir.as_deref().map(AsRef::as_ref),
            &|id| self.is_declared_instance(id),
        )?;
        let phase = StartupPhase::new("manager", DEFAULT_FLOW_INSTANCE_ID, "storage_hydrate");
        if let Err(err) = crate::storage_bridge::hydrate_runtime_from_storage(
            self.storage.as_ref(),
            &self.instances,
        )
        .await
        {
            phase.log_failure(&err);
            return Err(err);
        }
        phase.log_success();
        Ok(())
    }

    pub async fn try_acquire_pipeline_op(
        &self,
        pipeline_id: &str,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let semaphore = {
            let mut guard = self.pipeline_op_locks.lock().await;
            guard
                .entry(pipeline_id.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        semaphore.try_acquire_owned()
    }

    pub async fn try_acquire_stream_op(
        &self,
        stream_name: &str,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let semaphore = {
            let mut guard = self.stream_op_locks.lock().await;
            guard
                .entry(stream_name.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(STREAM_SHARED_REF_PERMITS as usize)))
                .clone()
        };
        semaphore.try_acquire_many_owned(STREAM_SHARED_REF_PERMITS)
    }

    pub async fn try_acquire_stream_ref_ops<I, S>(
        &self,
        stream_names: I,
    ) -> Result<Vec<OwnedSemaphorePermit>, TryAcquireError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let stream_names = stream_names
            .into_iter()
            .map(Into::into)
            .filter(|name: &String| !name.trim().is_empty())
            .collect::<BTreeSet<_>>();
        if stream_names.is_empty() {
            return Ok(Vec::new());
        }

        let semaphores = {
            let mut guard = self.stream_op_locks.lock().await;
            stream_names
                .into_iter()
                .map(|name| {
                    guard
                        .entry(name)
                        .or_insert_with(|| {
                            Arc::new(Semaphore::new(STREAM_SHARED_REF_PERMITS as usize))
                        })
                        .clone()
                })
                .collect::<Vec<_>>()
        };

        let mut permits = Vec::with_capacity(semaphores.len());
        for semaphore in semaphores {
            permits.push(semaphore.try_acquire_owned()?);
        }
        Ok(permits)
    }

    pub fn try_acquire_storage_operation(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.storage_operation_lock.clone().try_acquire_owned()
    }

    pub async fn try_acquire_shared_mqtt_ops<I, S>(
        &self,
        keys: I,
    ) -> Result<Vec<OwnedSemaphorePermit>, TryAcquireError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let keys = keys
            .into_iter()
            .map(Into::into)
            .filter(|key: &String| !key.trim().is_empty())
            .collect::<BTreeSet<_>>();
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let semaphores = {
            let mut guard = self.shared_mqtt_op_locks.lock().await;
            keys.into_iter()
                .map(|key| {
                    guard
                        .entry(key)
                        .or_insert_with(|| Arc::new(Semaphore::new(1)))
                        .clone()
                })
                .collect::<Vec<_>>()
        };

        let mut permits = Vec::with_capacity(semaphores.len());
        for semaphore in semaphores {
            permits.push(semaphore.try_acquire_owned()?);
        }
        Ok(permits)
    }
}

#[cfg(test)]
mod secret_propagation_tests {
    use super::*;
    use crate::instances::new_default_flow_instance;
    use flow::secret::{SecretContext, SecretPolicy, SecretRef, SecretStore};

    fn spec(id: &str) -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: id.to_string(),
            ..FlowInstanceSpec::default()
        }
    }

    #[test]
    fn secret_context_propagates_to_non_default_instances() {
        // Default instance carries a bootstrapped store with `k -> v`.
        let default = new_default_flow_instance();
        let mut store = SecretStore::empty();
        store.set("k", "v");
        default.set_secret_context(SecretContext::new(
            std::sync::Arc::new(store),
            SecretPolicy::Warn,
        ));

        let dir = tempfile::tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let app = AppState::new(
            default,
            storage,
            vec![spec(DEFAULT_FLOW_INSTANCE_ID), spec("worker_1")],
            0,
        )
        .expect("app state");

        // The non-default instance resolves the same store key.
        let worker = app.instances.get("worker_1").expect("worker instance");
        let ctx = worker.secret_context();
        let (value, _warn) = ctx
            .resolve(&SecretRef::store("k"), "test.field")
            .expect("resolve");
        assert_eq!(value.expose(), "v");
    }
}

#[cfg(test)]
mod config_validation_tests {
    use super::*;
    use crate::instances::new_default_flow_instance;

    fn spec(id: &str) -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: id.to_string(),
            ..FlowInstanceSpec::default()
        }
    }

    fn build(flow_instances: Vec<FlowInstanceSpec>) -> Result<AppState, String> {
        let dir = tempfile::tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        AppState::new(new_default_flow_instance(), storage, flow_instances, 0)
    }

    #[test]
    fn app_state_rejects_invalid_flow_instance_id() {
        let err = build(vec![spec(DEFAULT_FLOW_INSTANCE_ID), spec("bad-id")])
            .err()
            .expect("expected config error");
        assert!(err.contains("invalid flow_instances id"), "got: {err}");
    }

    #[test]
    fn app_state_rejects_duplicate_flow_instance_ids() {
        let err = build(vec![
            spec(DEFAULT_FLOW_INSTANCE_ID),
            spec("worker_1"),
            spec("worker_1"),
        ])
        .err()
        .expect("expected config error");
        assert!(
            err.contains("duplicate flow instance id in config"),
            "got: {err}"
        );
    }

    #[test]
    fn app_state_requires_a_default_instance() {
        let err = build(vec![spec("worker_1")])
            .err()
            .expect("expected config error");
        assert!(err.contains("must contain a default"), "got: {err}");
    }

    #[test]
    fn app_state_accepts_valid_distinct_ids() {
        let app = build(vec![spec(DEFAULT_FLOW_INSTANCE_ID), spec("worker_1")])
            .expect("valid config accepted");
        assert!(app.is_declared_instance("worker_1"));
    }
}
