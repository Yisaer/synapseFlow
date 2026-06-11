use crate::FlowInstanceSpec;
use crate::instances::{
    DEFAULT_FLOW_INSTANCE_ID, FlowInstances, build_in_process_flow_instance,
    find_default_flow_instance_spec,
};
use crate::startup::StartupPhase;
use std::collections::{BTreeSet, HashMap};
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
    import_export_op_lock: Arc<Semaphore>,
    pipeline_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    stream_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    shared_mqtt_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl AppState {
    pub fn new(
        instance: flow::FlowInstance,
        storage: StorageManager,
        flow_instances: Vec<FlowInstanceSpec>,
        patrol_interval_secs: u64,
    ) -> Result<Self, String> {
        let instances = FlowInstances::new(instance);
        let storage = Arc::new(storage);
        let mut declared_instances = HashMap::new();
        let state = Self {
            instances,
            storage,
            declared_instances: Arc::new(HashMap::new()),
            import_export_op_lock: Arc::new(Semaphore::new(1)),
            pipeline_op_locks: Arc::new(Mutex::new(HashMap::new())),
            stream_op_locks: Arc::new(Mutex::new(HashMap::new())),
            shared_mqtt_op_locks: Arc::new(Mutex::new(HashMap::new())),
        };

        find_default_flow_instance_spec(&flow_instances)?;

        for spec in &flow_instances {
            let id = spec.id.trim();
            if id.is_empty() {
                return Err("flow_instances contains an empty id".to_string());
            }
            if declared_instances.insert(id.to_string(), ()).is_some() {
                return Err(format!("duplicate flow instance id in config: {id}"));
            }
        }

        let shared_registries = state.instances.default_instance().shared_registries();
        for spec in &flow_instances {
            let id = spec.id.trim();
            if id == DEFAULT_FLOW_INSTANCE_ID {
                continue;
            }

            let instance = build_in_process_flow_instance(spec, Some(shared_registries.clone()))?;
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
        crate::init_process::apply_init_json_if_needed(self.storage.as_ref(), &|id| {
            self.is_declared_instance(id)
        })?;
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

    pub fn try_acquire_import_export_op(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.import_export_op_lock.clone().try_acquire_owned()
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
