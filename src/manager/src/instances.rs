use flow::{FlowInstance, FlowInstanceDedicatedRuntimeOptions, FlowInstanceSharedRegistries};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub const DEFAULT_FLOW_INSTANCE_ID: &str = "default";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum FlowInstanceBackendKind {
    #[default]
    InProcess,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowInstanceRuntimeSpec {
    pub worker_threads: Option<usize>,
    pub thread_name_prefix: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowInstanceCgroupSpec {
    pub thread_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct FlowInstanceSpec {
    pub id: String,
    pub backend: FlowInstanceBackendKind,
    pub runtime: FlowInstanceRuntimeSpec,
    pub cgroup: FlowInstanceCgroupSpec,
}

// Manually implemented instead of derived to avoid interaction between
// #[serde(default)] (struct-level) and #[derive(Default)]. Deriving Default
// changes how serde resolves defaults for container-level #[serde(default)].
#[allow(clippy::derivable_impls)]
impl Default for FlowInstanceSpec {
    fn default() -> Self {
        Self {
            id: String::new(),
            backend: FlowInstanceBackendKind::InProcess,
            runtime: FlowInstanceRuntimeSpec::default(),
            cgroup: FlowInstanceCgroupSpec::default(),
        }
    }
}

impl FlowInstanceSpec {
    pub fn thread_cgroup_path(&self) -> Option<&str> {
        self.cgroup.thread_path.as_deref()
    }
}

pub fn find_default_flow_instance_spec(
    flow_instances: &[FlowInstanceSpec],
) -> Result<&FlowInstanceSpec, String> {
    let mut default = None;
    for spec in flow_instances {
        crate::resource_id::validate_resource_id(
            crate::resource_id::ResourceIdKind::FlowInstanceId,
            &spec.id,
        )
        .map_err(|err| format!("invalid flow_instances id: {err}"))?;
        let id = spec.id.as_str();
        if id == DEFAULT_FLOW_INSTANCE_ID {
            if default.is_some() {
                return Err("flow_instances must contain exactly one default instance".to_string());
            }
            default = Some(spec);
        }
    }

    let default = default.ok_or_else(|| {
        "flow_instances must contain a default in_process flow instance".to_string()
    })?;
    if !matches!(default.backend, FlowInstanceBackendKind::InProcess) {
        return Err("default flow instance must use backend=in_process".to_string());
    }
    Ok(default)
}

pub fn build_in_process_flow_instance(
    spec: &FlowInstanceSpec,
    shared_registries: Option<FlowInstanceSharedRegistries>,
) -> Result<FlowInstance, String> {
    let id = spec.id.trim();
    if id.is_empty() {
        return Err("flow instance id must not be empty".to_string());
    }

    FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        id,
        shared_registries,
        FlowInstanceDedicatedRuntimeOptions {
            worker_threads: spec.runtime.worker_threads,
            thread_name_prefix: spec.runtime.thread_name_prefix.clone(),
            thread_cgroup_path: spec.thread_cgroup_path().map(|path| path.to_string()),
        },
    ))
    .map_err(|e| e.to_string())
}

// SAFETY: the default in-process spec is always valid; error here is a programming bug
#[allow(clippy::expect_used)]
pub fn new_default_flow_instance() -> FlowInstance {
    build_in_process_flow_instance(
        &FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..FlowInstanceSpec::default()
        },
        None,
    )
    .expect("build default flow instance")
}

#[derive(Clone)]
pub struct FlowInstances {
    instances: Arc<RwLock<HashMap<String, Arc<FlowInstance>>>>,
}

impl FlowInstances {
    pub fn new(default_instance: FlowInstance) -> Self {
        assert_eq!(
            default_instance.id(),
            DEFAULT_FLOW_INSTANCE_ID,
            "default FlowInstance must have id=default"
        );
        let mut map = HashMap::new();
        map.insert(
            DEFAULT_FLOW_INSTANCE_ID.to_string(),
            Arc::new(default_instance),
        );
        Self {
            instances: Arc::new(RwLock::new(map)),
        }
    }

    #[allow(clippy::expect_used)] // default instance always present; inserted unconditionally in new()
    pub fn default_instance(&self) -> Arc<FlowInstance> {
        self.get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default flow instance missing")
    }

    #[allow(dead_code)]
    pub fn insert_local_instance(&self, instance: FlowInstance) -> Option<Arc<FlowInstance>> {
        let id = instance.id().to_string();
        if id == DEFAULT_FLOW_INSTANCE_ID {
            return None;
        }
        self.instances.write().insert(id, Arc::new(instance))
    }

    pub fn get(&self, id: &str) -> Option<Arc<FlowInstance>> {
        self.instances.read().get(id).cloned()
    }

    pub fn instances_snapshot(&self) -> Vec<(String, Arc<FlowInstance>)> {
        let guard = self.instances.read();
        let mut out = guard
            .iter()
            .map(|(id, inst)| (id.clone(), Arc::clone(inst)))
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str) -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: id.to_string(),
            ..FlowInstanceSpec::default()
        }
    }

    #[test]
    fn find_default_accepts_valid_ids_with_single_default() {
        let specs = vec![spec(DEFAULT_FLOW_INSTANCE_ID), spec("worker_1")];
        let default = find_default_flow_instance_spec(&specs).expect("default found");
        assert_eq!(default.id, DEFAULT_FLOW_INSTANCE_ID);
    }

    #[test]
    fn find_default_rejects_invalid_resource_id() {
        let specs = vec![spec(DEFAULT_FLOW_INSTANCE_ID), spec("worker-1")];
        let err = find_default_flow_instance_spec(&specs).unwrap_err();
        assert!(err.contains("invalid flow_instances id"), "got: {err}");
        assert!(err.contains("flow_instance_id"), "got: {err}");
    }

    #[test]
    fn find_default_requires_a_default_instance() {
        let specs = vec![spec("worker_1")];
        let err = find_default_flow_instance_spec(&specs).unwrap_err();
        assert!(err.contains("must contain a default"), "got: {err}");
    }

    #[test]
    fn find_default_rejects_two_defaults() {
        let specs = vec![
            spec(DEFAULT_FLOW_INSTANCE_ID),
            spec(DEFAULT_FLOW_INSTANCE_ID),
        ];
        let err = find_default_flow_instance_spec(&specs).unwrap_err();
        assert!(err.contains("exactly one default"), "got: {err}");
    }
}
