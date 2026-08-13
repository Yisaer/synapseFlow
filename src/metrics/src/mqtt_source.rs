#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::IntCounterVec;

use crate::common::{opts, register_collector, CONNECTOR_LABEL, FLOW_INSTANCE_LABEL};

static MQTT_SOURCE_MESSAGES_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_source",
                "messages_in_total",
                "Number of messages received from MQTT sources",
            ),
            &[FLOW_INSTANCE_LABEL, CONNECTOR_LABEL],
        )
        .expect("create mqtt source messages_in counter vec"),
    )
});

static MQTT_SOURCE_MESSAGES_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_source",
                "messages_out_total",
                "Number of messages emitted downstream by MQTT sources",
            ),
            &[FLOW_INSTANCE_LABEL, CONNECTOR_LABEL],
        )
        .expect("create mqtt source messages_out counter vec"),
    )
});

pub fn messages_in_total() -> &'static IntCounterVec {
    &MQTT_SOURCE_MESSAGES_IN_TOTAL
}

pub fn messages_out_total() -> &'static IntCounterVec {
    &MQTT_SOURCE_MESSAGES_OUT_TOTAL
}
