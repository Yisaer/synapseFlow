#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::IntCounterVec;

use crate::common::{opts, register_collector, CONNECTOR_LABEL, FLOW_INSTANCE_LABEL};

static MQTT_SINK_MESSAGES_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_sink",
                "messages_in_total",
                "Number of messages received by MQTT sink connectors",
            ),
            &[FLOW_INSTANCE_LABEL, CONNECTOR_LABEL],
        )
        .expect("create mqtt sink messages_in counter vec"),
    )
});

static MQTT_SINK_MESSAGES_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_sink",
                "messages_out_total",
                "Number of messages successfully published by MQTT sink connectors",
            ),
            &[FLOW_INSTANCE_LABEL, CONNECTOR_LABEL],
        )
        .expect("create mqtt sink messages_out counter vec"),
    )
});

pub fn messages_in_total() -> &'static IntCounterVec {
    &MQTT_SINK_MESSAGES_IN_TOTAL
}

pub fn messages_out_total() -> &'static IntCounterVec {
    &MQTT_SINK_MESSAGES_OUT_TOTAL
}
