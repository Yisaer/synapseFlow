mod metrics;

use std::env;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;

use crate::metrics::{CPU_SECONDS_TOTAL_COUNTER, CPU_USAGE_GAUGE, MEMORY_USAGE_GAUGE};
use flow::codec::JsonDecoder;
use flow::connector::{MqttSinkConfig, MqttSinkConnector, MqttSourceConfig, MqttSourceConnector};
use flow::processor::processor_builder::PlanProcessor;
use flow::processor::{ProcessorPipeline, SinkProcessor};
use flow::JsonEncoder;
use flow::Processor;
use sysinfo::{Pid, System};
use tokio::time::{sleep, Duration};

const DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
const SOURCE_TOPIC: &str = "/yisa/data";
const SINK_TOPIC: &str = "/yisa/data2";
const MQTT_QOS: u8 = 1;
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9898";
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_metrics_exporter().await?;

    let sql = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: synapse-flow \"<SQL query>\"");
        process::exit(1);
    });

    let mut sink = SinkProcessor::new("mqtt_sink");
    sink.disable_result_forwarding();
    let sink_config = MqttSinkConfig::new("mqtt_sink", DEFAULT_BROKER_URL, SINK_TOPIC, MQTT_QOS);
    let sink_connector = MqttSinkConnector::new("mqtt_sink_connector", sink_config);
    sink.add_connector(
        Box::new(sink_connector),
        Arc::new(JsonEncoder::new("mqtt_sink_encoder")),
    );

    let mut pipeline = flow::create_pipeline(&sql, vec![sink])?;
    attach_mqtt_sources(&mut pipeline, DEFAULT_BROKER_URL, SOURCE_TOPIC, MQTT_QOS)?;

    if let Some(mut output_rx) = pipeline.take_output() {
        tokio::spawn(async move {
            while let Some(message) = output_rx.recv().await {
                println!("[PipelineOutput] {}", message.description());
            }
            println!("[PipelineOutput] channel closed");
        });
    } else {
        println!("[PipelineOutput] output channel unavailable; nothing will be drained");
    }

    pipeline.start();
    println!("Pipeline running between MQTT topics {SOURCE_TOPIC} -> {SINK_TOPIC} WITH SQL {sql}.");
    println!("Press Ctrl+C to terminate.");

    tokio::signal::ctrl_c().await?;
    println!("Stopping pipeline...");
    pipeline.quick_close().await?;
    Ok(())
}

async fn init_metrics_exporter() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = env::var("METRICS_ADDR")
        .unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string())
        .parse()?;
    let exporter = prometheus_exporter::start(addr)?;
    // Leak exporter handle so the HTTP endpoint stays alive for the duration of the process.
    Box::leak(Box::new(exporter));

    let poll_interval = env::var("METRICS_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    tokio::spawn(async move {
        let mut system = System::new();
        let pid = Pid::from_u32(process::id());
        loop {
            system.refresh_process(pid);
            if let Some(proc_info) = system.process(pid) {
                let cpu_usage_percent = proc_info.cpu_usage() as f64;
                CPU_USAGE_GAUGE.set(cpu_usage_percent as i64);
                let delta_secs = (cpu_usage_percent / 100.0) * poll_interval as f64;
                if delta_secs.is_finite() && delta_secs >= 0.0 {
                    CPU_SECONDS_TOTAL_COUNTER.inc_by(delta_secs);
                }
                MEMORY_USAGE_GAUGE.set(proc_info.memory() as i64);
            } else {
                CPU_USAGE_GAUGE.set(0);
                MEMORY_USAGE_GAUGE.set(0);
            }

            sleep(Duration::from_secs(poll_interval)).await;
        }
    });

    Ok(())
}

fn attach_mqtt_sources(
    pipeline: &mut ProcessorPipeline,
    broker_url: &str,
    topic: &str,
    qos: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut attached = false;
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let source_id = ds.id().to_string();
            let config = MqttSourceConfig::new(
                source_id.clone(),
                broker_url.to_string(),
                topic.to_string(),
                qos,
            );
            let connector =
                MqttSourceConnector::new(format!("{source_id}_source_connector"), config);
            let decoder = Arc::new(JsonDecoder::new(source_id));
            ds.add_connector(Box::new(connector), decoder);
            attached = true;
        }
    }

    if attached {
        Ok(())
    } else {
        Err("no datasource processors available to attach MQTT source".into())
    }
}
