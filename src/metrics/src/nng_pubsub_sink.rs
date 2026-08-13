#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::IntCounterVec;

use crate::common::{opts, register_collector, CONNECTOR_LABEL, FLOW_INSTANCE_LABEL};

static NNG_PUBSUB_SINK_MESSAGES_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "nng_pubsub_sink",
                "messages_in_total",
                "Number of messages received by NNG pubsub sink connectors",
            ),
            &[FLOW_INSTANCE_LABEL, CONNECTOR_LABEL],
        )
        .expect("create nng pubsub sink messages_in counter vec"),
    )
});

static NNG_PUBSUB_SINK_MESSAGES_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "nng_pubsub_sink",
                "messages_out_total",
                "Number of messages successfully published by NNG pubsub sink connectors",
            ),
            &[FLOW_INSTANCE_LABEL, CONNECTOR_LABEL],
        )
        .expect("create nng pubsub sink messages_out counter vec"),
    )
});

pub fn messages_in_total() -> &'static IntCounterVec {
    &NNG_PUBSUB_SINK_MESSAGES_IN_TOTAL
}

pub fn messages_out_total() -> &'static IntCounterVec {
    &NNG_PUBSUB_SINK_MESSAGES_OUT_TOTAL
}
