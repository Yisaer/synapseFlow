use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::{CatalogError, StreamDefinition, StreamProps};
use crate::shared_stream::{SharedStreamConfig, SharedStreamError, SharedStreamInfo};

use super::{FlowInstance, FlowInstanceError, StreamRuntimeInfo};

impl FlowInstance {
    /// Create a stream definition and optionally attach a shared stream runtime.
    pub async fn create_stream(
        &self,
        definition: StreamDefinition,
        shared: bool,
    ) -> Result<StreamRuntimeInfo, FlowInstanceError> {
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

    /// Delete a stream definition and its shared runtime (if registered).
    pub async fn delete_stream(&self, name: &str) -> Result<(), FlowInstanceError> {
        if self.shared_stream_registry.is_registered(name).await {
            self.shared_stream_registry.drop_stream(name).await?;
        }
        self.catalog.remove(name)?;
        Ok(())
    }

    async fn ensure_shared_stream(
        &self,
        definition: Arc<StreamDefinition>,
    ) -> Result<SharedStreamInfo, FlowInstanceError> {
        match definition.props() {
            StreamProps::Mqtt(props) => {
                let mut config = SharedStreamConfig::new(definition.id(), definition.schema());
                struct MqttSharedStreamConnectorFactory {
                    stream_id: String,
                    schema: Arc<datatypes::Schema>,
                    decoder: crate::catalog::StreamDecoderConfig,
                    broker_url: String,
                    topic: String,
                    qos: u8,
                    client_id: Option<String>,
                    connector_key: Option<String>,
                    mqtt_client_manager: crate::connector::MqttClientManager,
                    decoder_registry: Arc<crate::codec::DecoderRegistry>,
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
                            self.mqtt_client_manager.clone(),
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
                    schema: definition.schema(),
                    decoder: definition.decoder().clone(),
                    broker_url: props.broker_url.clone(),
                    topic: props.topic.clone(),
                    qos: props.qos,
                    client_id: props.client_id.clone(),
                    connector_key: props.connector_key.clone(),
                    mqtt_client_manager: self.mqtt_client_manager.clone(),
                    decoder_registry: Arc::clone(&self.decoder_registry),
                });

                config.set_connector_factory(factory);
                self.shared_stream_registry
                    .create_stream(config)
                    .await
                    .map_err(FlowInstanceError::from)
            }
            StreamProps::Mock(_) => Err(FlowInstanceError::Invalid(
                "mock stream props cannot be used to create shared streams".to_string(),
            )),
            StreamProps::History(_) => Err(FlowInstanceError::Invalid(
                "history stream props cannot be used to create shared streams".to_string(),
            )),
        }
    }
}
