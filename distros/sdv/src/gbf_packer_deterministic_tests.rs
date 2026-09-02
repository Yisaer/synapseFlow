#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::decoder::can::CanIdMapping;
    use crate::schema::dbc::{CompiledDbcSchema, load_dbc_json};
    use crate::schema::gbf::{CompiledGbfSchema, GbfSchema};
    use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
    use flow::planner::sink::{
        CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
        SinkEncoderConfig,
    };
    use flow::processor::{EncodedDeliveryFlags, SamplerConfig, SamplingStrategy, StreamData};
    use flow::{FlowInstance, FlowInstanceOptions};
    use serde_json::{Map, Value, json};
    use tokio::time::{advance, timeout};

    fn gbf_schema() -> GbfSchema {
        serde_json::from_value(json!({
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "total_len",
                        "length_unit": "bytes",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "magic", "type": "u8", "const": 85 },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "data_len", "type": "u8" },
                                {
                                    "name": "payload",
                                    "type": "bytes",
                                    "length_ref": "data_len",
                                    "format": { "type": "dbc", "id_ref": "can_id" }
                                }
                            ]
                        }
                    }
                ]
            }
        }))
        .expect("parse GBF schema")
    }

    fn gbf_packet(timestamp: u64, frames: &[(u16, &[u8])]) -> Vec<u8> {
        let frame_size = 1 + 2 + 1;
        let frames_len: usize = frames
            .iter()
            .map(|(_, payload)| frame_size + payload.len())
            .sum();
        let mut packet = Vec::with_capacity(10 + frames_len);
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&(frames_len as u16).to_be_bytes());
        for (can_id, payload) in frames {
            packet.push(0x55);
            packet.extend_from_slice(&can_id.to_be_bytes());
            packet.push(payload.len() as u8);
            packet.extend_from_slice(payload);
        }
        packet
    }

    fn sink() -> PipelineSink {
        let connector = PipelineSinkConnector::new(
            "test_sink",
            SinkConnectorConfig::Nop(NopSinkConfig::default()),
            SinkEncoderConfig::json(),
        );
        PipelineSink::new("output_sink", connector)
            .with_forward_to_result(true)
            .with_common_props(CommonSinkProps::default())
    }

    async fn receive_json(receiver: &mut tokio::sync::mpsc::Receiver<StreamData>) -> Value {
        let mut delivery = Vec::new();
        let mut active = false;
        timeout(Duration::from_millis(100), async {
            loop {
                match receiver
                    .recv()
                    .await
                    .expect("pipeline output channel closed")
                {
                    StreamData::EncodedDelivery { flags, bytes } => {
                        if flags.contains(EncodedDeliveryFlags::START) {
                            delivery.clear();
                            active = true;
                        }
                        assert!(active, "received encoded data before START");
                        delivery.extend_from_slice(&bytes);
                        if flags.contains(EncodedDeliveryFlags::END) {
                            return serde_json::from_slice(&delivery).expect("decode JSON output");
                        }
                    }
                    StreamData::Control(_) | StreamData::Watermark(_) => {}
                    StreamData::Error(error) => {
                        panic!("pipeline returned error: {}", error.message)
                    }
                    other => panic!("unexpected pipeline output: {}", other.description()),
                }
            }
        })
        .await
        .expect("timeout waiting for deterministic GBF sampler output")
    }

    #[tokio::test(start_paused = true)]
    async fn gbf_packer_merges_latest_frames_in_one_sampler_window() {
        let instance =
            FlowInstance::new(FlowInstanceOptions::shared_current_runtime("default", None))
                .expect("create flow instance");
        crate::register(&instance);

        let dbc_path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(dbc_path.to_str().expect("DBC path")).expect("load DBC");
        let compiled_dbc =
            Arc::new(CompiledDbcSchema::new(dbc, "{sig_name}").expect("compile DBC schema"));
        let schema = Arc::new(compiled_dbc.schema("can"));
        let compiled_gbf = Arc::new(
            CompiledGbfSchema::can(
                gbf_schema(),
                compiled_dbc,
                true,
                CanIdMapping::BusShift { bits: 12 },
            )
            .expect("compile GBF schema"),
        );
        let decoder =
            StreamDecoderConfig::new("gbf", Map::new()).with_schema_artifact(compiled_gbf);
        let sampler = SamplerConfig {
            interval: Duration::from_millis(500),
            strategy: SamplingStrategy::Packer {
                props: flow::processor::sampler_processor::PackerProps {
                    merger: flow::processor::sampler_processor::MergerConfig {
                        merger_type: "gbf".to_string(),
                        props: Map::new(),
                    },
                },
            },
        };
        instance
            .create_stream(
                StreamDefinition::new(
                    "can",
                    schema,
                    StreamProps::Mock(MockStreamProps::default()),
                    decoder,
                )
                .with_sampler(sampler),
                false,
            )
            .await
            .expect("create GBF mock stream");

        let mut pipeline = instance
            .build_pipeline("SELECT * FROM can", vec![sink()])
            .expect("build pipeline");
        let mut output = pipeline.take_output().expect("take pipeline output");
        pipeline.start().await.expect("start pipeline");
        tokio::task::yield_now().await;

        let original = hex::decode("0854657374000011").expect("decode original payload");
        let override_mess1 = hex::decode("08AABBCCDD000011").expect("decode Mess1 payload");
        let override_mess0 = hex::decode("08EEFF00112200FF").expect("decode Mess0 payload");
        pipeline
            .send_stream_data(
                "can",
                StreamData::bytes(gbf_packet(
                    1_720_765_705_290,
                    &[(0x1586, &original), (0x124A, &original)],
                )),
            )
            .await
            .expect("send packet 1");
        pipeline
            .send_stream_data(
                "can",
                StreamData::bytes(gbf_packet(1_720_765_705_300, &[(0x1586, &override_mess1)])),
            )
            .await
            .expect("send packet 2");
        pipeline
            .send_stream_data(
                "can",
                StreamData::bytes(gbf_packet(1_720_765_705_310, &[(0x124A, &override_mess0)])),
            )
            .await
            .expect("send packet 3");

        let sampler_stats = pipeline
            .processor_stats()
            .into_iter()
            .find(|stats| stats.processor_id.to_lowercase().contains("sampler"))
            .expect("sampler stats");
        for _ in 0..20 {
            if sampler_stats.snapshot().stats.custom["messages_in"] == 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(sampler_stats.snapshot().stats.custom["messages_in"], 3);

        advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        let result = receive_json(&mut output).await;
        let rows = result.as_array().expect("columnar JSON output is an array");
        assert_eq!(rows.len(), 1, "expected one merged row");
        assert_eq!(rows[0]["ts"], 1_720_765_705_310_i64);
        assert_eq!(rows[0]["Mess0_Sig1"], 238);
        assert_eq!(rows[0]["Mess1_Sig1"], 170);
        assert_eq!(sampler_stats.snapshot().stats.records_out, Some(1));

        timeout(
            Duration::from_millis(100),
            pipeline.close(Duration::from_secs(1)),
        )
        .await
        .expect("pipeline close should not hang")
        .expect("close pipeline");
    }
}
