use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

use crate::catalog::{CatalogError, StreamDefinition, StreamProps};
use crate::connector::{MemoryTopicKind, MockSourceConnector};
use crate::shared_stream::{
    SharedStreamConfig, SharedStreamError, SharedStreamInfo, SharedStreamProcessorStats,
};

use super::{FlowInstance, FlowInstanceError, StreamRuntimeInfo};

impl FlowInstance {
    /// Retrieve a stream definition from the local catalog.
    pub fn stream_definition(&self, name: &str) -> Option<Arc<StreamDefinition>> {
        self.catalog.get(name)
    }

    /// Create a stream definition and optionally attach a shared stream runtime.
    pub async fn create_stream(
        &self,
        definition: StreamDefinition,
        shared: bool,
    ) -> Result<StreamRuntimeInfo, FlowInstanceError> {
        self.validate_stream_definition(&definition)?;
        let stored = self.catalog.insert(definition)?;
        let shared_info = if shared {
            match self.ensure_shared_stream(stored.clone()).await {
                Ok(info) => Some(info),
                Err(err) => {
                    let _ = self.catalog.remove(stored.id());
                    return Err(err);
                }
            }
        } else {
            None
        };
        Ok(StreamRuntimeInfo {
            definition: stored,
            shared_info,
        })
    }

    fn validate_stream_definition(
        &self,
        definition: &StreamDefinition,
    ) -> Result<(), FlowInstanceError> {
        let StreamProps::Memory(props) = definition.props() else {
            return Ok(());
        };

        let topic = props.topic.trim();
        if topic.is_empty() {
            return Err(FlowInstanceError::Invalid(format!(
                "stream '{}' memory topic must not be empty",
                definition.id()
            )));
        }

        let expected_kind = if definition.decoder().kind() == "none" {
            MemoryTopicKind::Collection
        } else {
            MemoryTopicKind::Bytes
        };
        let actual_kind = self.memory_topic_kind(topic).ok_or_else(|| {
            FlowInstanceError::Invalid(format!(
                "memory topic '{}' required by stream '{}' not declared in instance",
                topic,
                definition.id()
            ))
        })?;
        if actual_kind != expected_kind {
            return Err(FlowInstanceError::Invalid(format!(
                "memory topic '{}' kind mismatch for stream '{}': expected {}, got {}",
                topic,
                definition.id(),
                expected_kind,
                actual_kind
            )));
        }

        Ok(())
    }

    /// Retrieve a stream definition and its shared runtime (if any).
    pub async fn get_stream(&self, name: &str) -> Result<StreamRuntimeInfo, FlowInstanceError> {
        let definition = self
            .catalog
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        let shared_info = match self.shared_stream_registry.get_stream(name).await {
            Ok(info) => Some(info),
            Err(SharedStreamError::NotFound(_)) => None,
            Err(err) => return Err(err.into()),
        };
        Ok(StreamRuntimeInfo {
            definition,
            shared_info,
        })
    }

    /// List all streams with their shared runtime metadata.
    pub async fn list_streams(&self) -> Result<Vec<StreamRuntimeInfo>, FlowInstanceError> {
        let shared_infos = self.shared_stream_registry.list_streams().await;
        let shared_map: HashMap<String, SharedStreamInfo> = shared_infos
            .into_iter()
            .map(|info| (info.name.clone(), info))
            .collect();

        let mut payload = Vec::new();
        for definition in self.catalog.list() {
            let shared_info = shared_map.get(definition.id()).cloned();
            payload.push(StreamRuntimeInfo {
                definition,
                shared_info,
            });
        }
        Ok(payload)
    }

    /// Fetch processor stats for one shared stream's internal ingest pipeline.
    pub async fn get_shared_stream_processor_stats(
        &self,
        name: &str,
    ) -> Result<SharedStreamProcessorStats, FlowInstanceError> {
        self.catalog
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;

        match self
            .shared_stream_registry
            .get_stream_processor_stats(name)
            .await
        {
            Ok(stats) => Ok(stats),
            Err(SharedStreamError::NotFound(_)) => Err(FlowInstanceError::Invalid(format!(
                "stream {name} is not a shared stream"
            ))),
            Err(err) => Err(err.into()),
        }
    }

    /// Replace a stream definition and optionally rebuild its shared runtime.
    ///
    /// Unlike `create_stream`, this does not check whether pipelines reference the
    /// stream.  Callers MUST verify that the update is safe (e.g. no running
    /// pipelines are consuming a shared stream) before calling this method.
    ///
    /// For non-shared streams the catalog entry is swapped and running pipelines are
    /// unaffected (they hold their own source connectors).  For shared streams the
    /// existing shared runtime is torn down and recreated; any active subscribers
    /// will have their channels closed.
    pub async fn replace_stream(
        &self,
        definition: StreamDefinition,
        shared: bool,
    ) -> Result<StreamRuntimeInfo, FlowInstanceError> {
        self.validate_stream_definition(&definition)?;

        // Drop existing shared runtime if registered.
        if self
            .shared_stream_registry
            .is_registered(definition.id())
            .await
        {
            self.shared_stream_registry
                .drop_stream(definition.id())
                .await?;
        }
        self.shared_mock_source_handles
            .lock()
            .remove(definition.id());

        // Swap the catalog entry (upsert overwrites regardless of prior existence).
        let stored = self.catalog.upsert(definition);

        let shared_info = if shared {
            Some(self.ensure_shared_stream(stored.clone()).await?)
        } else {
            None
        };

        Ok(StreamRuntimeInfo {
            definition: stored,
            shared_info,
        })
    }

    /// Delete a stream definition and its shared runtime (if registered).
    pub async fn delete_stream(&self, name: &str) -> Result<(), FlowInstanceError> {
        let pipelines_using_stream = self
            .pipeline_manager
            .list()
            .into_iter()
            .filter(|snapshot| snapshot.streams.iter().any(|stream| stream == name))
            .map(|snapshot| snapshot.definition.id().to_string())
            .collect::<Vec<_>>();
        if !pipelines_using_stream.is_empty() {
            return Err(FlowInstanceError::StreamInUse {
                stream: name.to_string(),
                pipelines: pipelines_using_stream.join(", "),
            });
        }

        if self.shared_stream_registry.is_registered(name).await {
            self.shared_stream_registry.drop_stream(name).await?;
        }
        self.shared_mock_source_handles.lock().remove(name);
        self.catalog.remove(name)?;
        Ok(())
    }

    /// Send one payload into a shared mock stream.
    ///
    /// This is intended for tests and local diagnostics that need to drive a shared mock stream
    /// through the normal shared-stream runtime path.
    pub async fn send_shared_mock_stream_payload(
        &self,
        name: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), FlowInstanceError> {
        let definition = self
            .catalog
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        if !self.shared_stream_registry.is_registered(name).await {
            return Err(FlowInstanceError::Invalid(format!(
                "stream {name} is not a shared stream"
            )));
        }
        if !matches!(definition.props(), StreamProps::Mock(_)) {
            return Err(FlowInstanceError::Invalid(format!(
                "stream {name} is not a shared mock stream"
            )));
        }

        let payload = payload.into();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let handle = {
                let guard = self.shared_mock_source_handles.lock();
                guard.get(name).cloned()
            };
            if let Some(handle) = handle {
                handle.send(payload.clone()).await.map_err(|err| {
                    FlowInstanceError::Invalid(format!(
                        "send payload to shared mock stream {name}: {err}"
                    ))
                })?;
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(FlowInstanceError::Invalid(format!(
                    "shared mock stream {name} is not ready to receive payloads"
                )));
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn ensure_shared_stream(
        &self,
        definition: Arc<StreamDefinition>,
    ) -> Result<SharedStreamInfo, FlowInstanceError> {
        match definition.props() {
            StreamProps::Mqtt(props) => {
                let mut config = SharedStreamConfig::new(definition.id(), definition.schema());
                config.flow_instance_id = Arc::<str>::from(self.id.as_str());
                config.set_decoder(definition.decoder().clone());
                struct MqttSharedStreamConnectorFactory {
                    stream_id: String,
                    flow_instance_id: Arc<str>,
                    schema: Arc<datatypes::Schema>,
                    decoder: crate::catalog::StreamDecoderConfig,
                    broker_url: String,
                    topic: String,
                    qos: u8,
                    client_id: Option<String>,
                    connector_key: Option<String>,
                    mqtt_client_manager: crate::connector::MqttClientManager,
                    decoder_registry: Arc<crate::codec::DecoderRegistry>,
                    spawner: crate::runtime::TaskSpawner,
                }

                impl crate::shared_stream::SharedStreamConnectorFactory for MqttSharedStreamConnectorFactory {
                    fn connector_id(&self) -> String {
                        format!("{}_shared_source_connector", self.stream_id)
                    }

                    fn build(
                        &self,
                    ) -> Result<
                        (
                            Box<dyn crate::connector::SourceConnector>,
                            Arc<dyn crate::codec::RecordDecoder>,
                        ),
                        crate::shared_stream::SharedStreamError,
                    > {
                        let mut source_config = crate::connector::MqttSourceConfig::new(
                            format!("{}_shared_source", self.stream_id),
                            self.broker_url.clone(),
                            self.topic.clone(),
                            self.qos,
                        );
                        if let Some(client_id) = &self.client_id {
                            source_config = source_config.with_client_id(client_id.clone());
                        }
                        if let Some(connector_key) = &self.connector_key {
                            source_config = source_config.with_connector_key(connector_key.clone());
                        }
                        let connector = crate::connector::MqttSourceConnector::new(
                            self.connector_id(),
                            source_config,
                            self.flow_instance_id.as_ref(),
                            self.mqtt_client_manager.clone(),
                            self.spawner.clone(),
                        );
                        let decoder = self.decoder_registry.instantiate(
                            &self.decoder,
                            &self.stream_id,
                            Arc::clone(&self.schema),
                        );
                        let decoder = decoder.map_err(|err| {
                            crate::shared_stream::SharedStreamError::Internal(err.to_string())
                        })?;
                        Ok((Box::new(connector), decoder))
                    }
                }

                let factory = Arc::new(MqttSharedStreamConnectorFactory {
                    stream_id: definition.id().to_string(),
                    flow_instance_id: Arc::<str>::from(self.id.as_str()),
                    schema: definition.schema(),
                    decoder: definition.decoder().clone(),
                    broker_url: props.broker_url.clone(),
                    topic: props.topic.clone(),
                    qos: props.qos,
                    client_id: props.client_id.clone(),
                    connector_key: props.connector_key.clone(),
                    mqtt_client_manager: self.mqtt_client_manager.clone(),
                    decoder_registry: Arc::clone(&self.decoder_registry),
                    spawner: self.spawner.clone(),
                });

                config.set_connector_factory(factory);
                if let Some(sampler) = definition.sampler() {
                    config.set_sampler(sampler.clone());
                }
                self.shared_stream_registry
                    .create_stream(config)
                    .await
                    .map_err(FlowInstanceError::from)
            }
            StreamProps::Mock(_) => {
                let mut config = SharedStreamConfig::new(definition.id(), definition.schema());
                config.set_decoder(definition.decoder().clone());

                struct MockSharedStreamConnectorFactory {
                    stream_id: String,
                    schema: Arc<datatypes::Schema>,
                    decoder: crate::catalog::StreamDecoderConfig,
                    decoder_registry: Arc<crate::codec::DecoderRegistry>,
                    handle_store: Arc<
                        parking_lot::Mutex<HashMap<String, crate::connector::MockSourceHandle>>,
                    >,
                }

                impl crate::shared_stream::SharedStreamConnectorFactory for MockSharedStreamConnectorFactory {
                    fn connector_id(&self) -> String {
                        format!("{}_shared_source_connector", self.stream_id)
                    }

                    fn build(
                        &self,
                    ) -> Result<
                        (
                            Box<dyn crate::connector::SourceConnector>,
                            Arc<dyn crate::codec::RecordDecoder>,
                        ),
                        crate::shared_stream::SharedStreamError,
                    > {
                        let (connector, handle) = MockSourceConnector::new(self.connector_id());
                        self.handle_store
                            .lock()
                            .insert(self.stream_id.clone(), handle);

                        let decoder = self.decoder_registry.instantiate(
                            &self.decoder,
                            &self.stream_id,
                            Arc::clone(&self.schema),
                        );
                        let decoder = decoder.map_err(|err| {
                            crate::shared_stream::SharedStreamError::Internal(err.to_string())
                        })?;
                        Ok((Box::new(connector), decoder))
                    }
                }

                let factory = Arc::new(MockSharedStreamConnectorFactory {
                    stream_id: definition.id().to_string(),
                    schema: definition.schema(),
                    decoder: definition.decoder().clone(),
                    decoder_registry: Arc::clone(&self.decoder_registry),
                    handle_store: Arc::clone(&self.shared_mock_source_handles),
                });

                config.set_connector_factory(factory);
                if let Some(sampler) = definition.sampler() {
                    config.set_sampler(sampler.clone());
                }
                self.shared_stream_registry
                    .create_stream(config)
                    .await
                    .map_err(FlowInstanceError::from)
            }
            StreamProps::History(_) => Err(FlowInstanceError::Invalid(
                "history stream props cannot be used to create shared streams".to_string(),
            )),
            StreamProps::Memory(_) => Err(FlowInstanceError::Invalid(
                "memory stream props cannot be used to create shared streams".to_string(),
            )),
            StreamProps::Video(_) => Err(FlowInstanceError::Invalid(
                "video stream props cannot be used to create shared streams".to_string(),
            )),
            StreamProps::NngPubSub(_) => Err(FlowInstanceError::Invalid(
                "nng_pubsub stream props cannot be used to create shared streams".to_string(),
            )),
        }
    }
}
