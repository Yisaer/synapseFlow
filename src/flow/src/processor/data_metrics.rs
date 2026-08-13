use crate::planner::physical::DataDomain;
use crate::processor::{
    CounterHandle, MetricKind, MetricSpec, ProcessorError, ProcessorStats, StreamData,
};

macro_rules! counter_spec {
    ($owner:literal, $name:literal) => {
        MetricSpec {
            id: concat!($owner, ".", $name),
            flat_name: $name,
            kind: MetricKind::Counter,
        }
    };
}

pub(crate) struct EncoderMetrics {
    messages_out: CounterHandle,
    messages_aborted: CounterHandle,
    bytes_out: CounterHandle,
}

impl EncoderMetrics {
    pub(crate) fn new(stats: &ProcessorStats) -> Self {
        stats.declare_collection_in();
        Self {
            messages_out: stats.register_counter(counter_spec!("sink_encoder", "messages_out")),
            messages_aborted: stats
                .register_counter(counter_spec!("sink_encoder", "messages_aborted")),
            bytes_out: stats.register_counter(counter_spec!("sink_encoder", "bytes_out")),
        }
    }

    pub(crate) fn record_output_bytes(&self, bytes: usize) {
        self.bytes_out.inc_by(bytes as u64);
    }

    pub(crate) fn record_message_out(&self) {
        self.messages_out.inc_by(1);
    }

    pub(crate) fn record_message_aborted(&self) {
        self.messages_aborted.inc_by(1);
    }
}

pub(crate) struct SinkMetrics {
    messages_in: Option<CounterHandle>,
    messages_out: CounterHandle,
    messages_dropped: CounterHandle,
    bytes_in: Option<CounterHandle>,
    bytes_delivered: Option<CounterHandle>,
}

impl SinkMetrics {
    pub(crate) fn new(stats: &ProcessorStats, input_domain: DataDomain) -> Self {
        let (bytes_in, bytes_delivered) = match input_domain {
            DataDomain::Message => (
                Some(stats.register_counter(counter_spec!("sink", "bytes_in"))),
                Some(stats.register_counter(counter_spec!("sink", "bytes_delivered"))),
            ),
            DataDomain::Collection => {
                stats.declare_collection_in();
                (None, None)
            }
        };
        Self {
            messages_in: (input_domain == DataDomain::Message)
                .then(|| stats.register_counter(counter_spec!("sink", "messages_in"))),
            messages_out: stats.register_counter(counter_spec!("sink", "messages_out")),
            messages_dropped: stats.register_counter(counter_spec!("sink", "messages_dropped")),
            bytes_in,
            bytes_delivered,
        }
    }

    pub(crate) fn record_encoded_message_in(&self) {
        self.messages_in
            .as_ref()
            .expect("encoded sink must declare messages_in")
            .inc_by(1);
    }

    pub(crate) fn record_input_bytes(&self, bytes: usize) {
        self.bytes_in
            .as_ref()
            .expect("encoded sink must declare bytes_in")
            .inc_by(bytes as u64);
    }

    pub(crate) fn record_encoded_output(&self, bytes: u64) {
        self.messages_out.inc_by(1);
        self.bytes_delivered
            .as_ref()
            .expect("encoded sink must declare bytes_delivered")
            .inc_by(bytes);
    }

    pub(crate) fn record_message_out(&self) {
        self.messages_out.inc_by(1);
    }

    pub(crate) fn record_dropped(&self) {
        self.messages_dropped.inc_by(1);
    }
}

pub(crate) struct TransformMetrics {
    messages_in: CounterHandle,
    messages_out: CounterHandle,
    messages_aborted: CounterHandle,
    bytes_in: CounterHandle,
    bytes_out: CounterHandle,
}

#[derive(Clone, Copy)]
pub(crate) struct TransformMetricSpecs {
    messages_in: MetricSpec,
    messages_out: MetricSpec,
    messages_aborted: MetricSpec,
    bytes_in: MetricSpec,
    bytes_out: MetricSpec,
}

macro_rules! transform_specs {
    ($owner:literal) => {
        TransformMetricSpecs {
            messages_in: counter_spec!($owner, "messages_in"),
            messages_out: counter_spec!($owner, "messages_out"),
            messages_aborted: counter_spec!($owner, "messages_aborted"),
            bytes_in: counter_spec!($owner, "bytes_in"),
            bytes_out: counter_spec!($owner, "bytes_out"),
        }
    };
}

pub(crate) const SINK_COMPRESS_METRICS: TransformMetricSpecs = transform_specs!("sink_compress");
pub(crate) const SINK_ENCRYPT_METRICS: TransformMetricSpecs = transform_specs!("sink_encrypt");

impl TransformMetrics {
    pub(crate) fn new(stats: &ProcessorStats, specs: TransformMetricSpecs) -> Self {
        Self {
            messages_in: stats.register_counter(specs.messages_in),
            messages_out: stats.register_counter(specs.messages_out),
            messages_aborted: stats.register_counter(specs.messages_aborted),
            bytes_in: stats.register_counter(specs.bytes_in),
            bytes_out: stats.register_counter(specs.bytes_out),
        }
    }

    pub(crate) fn record_input_bytes(&self, bytes: usize) {
        self.bytes_in.inc_by(bytes as u64);
    }

    pub(crate) fn record_output_bytes(&self, bytes: usize) {
        self.bytes_out.inc_by(bytes as u64);
    }

    pub(crate) fn record_message_in(&self) {
        self.messages_in.inc_by(1);
    }

    pub(crate) fn record_message_out(&self) {
        self.messages_out.inc_by(1);
    }

    pub(crate) fn record_aborted(&self) {
        self.messages_aborted.inc_by(1);
    }
}

/// Metrics for processors that can pass either raw messages or collections.
/// Encoded delivery frames are intentionally excluded because frame boundaries
/// are not message boundaries; their owner must record message lifecycle events.
pub(crate) struct DataMetrics {
    messages_in: Option<CounterHandle>,
    messages_out: Option<CounterHandle>,
    collections_in: bool,
    collections_out: bool,
    bytes_in: Option<CounterHandle>,
    bytes_out: Option<CounterHandle>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DataMetricDomains {
    messages_in: bool,
    messages_out: bool,
    collections_in: bool,
    collections_out: bool,
}

impl DataMetricDomains {
    pub(crate) const NONE: Self = Self {
        messages_in: false,
        messages_out: false,
        collections_in: false,
        collections_out: false,
    };

    pub(crate) const fn with_input(mut self, domain: DataDomain) -> Self {
        match domain {
            DataDomain::Message => self.messages_in = true,
            DataDomain::Collection => self.collections_in = true,
        }
        self
    }

    pub(crate) const fn with_output(mut self, domain: DataDomain) -> Self {
        match domain {
            DataDomain::Message => self.messages_out = true,
            DataDomain::Collection => self.collections_out = true,
        }
        self
    }

    pub(crate) const fn passthrough(self, domain: DataDomain) -> Self {
        self.with_input(domain).with_output(domain)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DataMetricSpecs {
    messages_in: MetricSpec,
    messages_out: MetricSpec,
    bytes_in: MetricSpec,
    bytes_out: MetricSpec,
}

macro_rules! data_specs {
    ($owner:literal) => {
        DataMetricSpecs {
            messages_in: counter_spec!($owner, "messages_in"),
            messages_out: counter_spec!($owner, "messages_out"),
            bytes_in: counter_spec!($owner, "bytes_in"),
            bytes_out: counter_spec!($owner, "bytes_out"),
        }
    };
}

pub(crate) const DATASOURCE_METRICS: DataMetricSpecs = data_specs!("datasource");
pub(crate) const DECODER_METRICS: DataMetricSpecs = data_specs!("decoder");
pub(crate) const SAMPLER_METRICS: DataMetricSpecs = data_specs!("sampler");
pub(crate) const BARRIER_METRICS: DataMetricSpecs = data_specs!("barrier");
pub(crate) const RESULT_COLLECT_METRICS: DataMetricSpecs = data_specs!("result_collect");

#[derive(Clone, Copy)]
pub(crate) enum DataMeasurement {
    Message { bytes: u64 },
    Collection { rows: u64 },
    Other,
}

impl DataMetrics {
    pub(crate) fn new(
        stats: &ProcessorStats,
        specs: DataMetricSpecs,
        domains: DataMetricDomains,
    ) -> Self {
        if domains.collections_in {
            stats.declare_collection_in();
        }
        if domains.collections_out {
            stats.declare_collection_out();
        }
        Self {
            messages_in: domains
                .messages_in
                .then(|| stats.register_counter(specs.messages_in)),
            messages_out: domains
                .messages_out
                .then(|| stats.register_counter(specs.messages_out)),
            collections_in: domains.collections_in,
            collections_out: domains.collections_out,
            bytes_in: domains
                .messages_in
                .then(|| stats.register_counter(specs.bytes_in)),
            bytes_out: domains
                .messages_out
                .then(|| stats.register_counter(specs.bytes_out)),
        }
    }

    pub(crate) fn measure(data: &StreamData) -> DataMeasurement {
        match data {
            StreamData::Bytes(bytes) => DataMeasurement::Message {
                bytes: bytes.len() as u64,
            },
            StreamData::Collection(collection) => DataMeasurement::Collection {
                rows: collection.num_rows() as u64,
            },
            _ => DataMeasurement::Other,
        }
    }

    pub(crate) fn record_input(
        &self,
        stats: &ProcessorStats,
        data: &StreamData,
    ) -> Result<DataMeasurement, ProcessorError> {
        let measurement = Self::measure(data);
        match measurement {
            DataMeasurement::Message { bytes } => {
                let messages_in = self.messages_in.as_ref().ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "received message outside the planned input domain".to_string(),
                    )
                })?;
                let bytes_in = self.bytes_in.as_ref().ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "received bytes outside the planned input domain".to_string(),
                    )
                })?;
                messages_in.inc_by(1);
                bytes_in.inc_by(bytes);
            }
            DataMeasurement::Collection { rows } => {
                if !self.collections_in {
                    return Err(ProcessorError::InvalidConfiguration(
                        "received collection outside the planned input domain".to_string(),
                    ));
                }
                stats.record_collection_in(rows);
            }
            DataMeasurement::Other => {}
        }
        Ok(measurement)
    }

    pub(crate) fn record_output(
        &self,
        stats: &ProcessorStats,
        measurement: DataMeasurement,
    ) -> Result<(), ProcessorError> {
        match measurement {
            DataMeasurement::Message { bytes } => {
                let messages_out = self.messages_out.as_ref().ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "produced message outside the planned output domain".to_string(),
                    )
                })?;
                let bytes_out = self.bytes_out.as_ref().ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "produced bytes outside the planned output domain".to_string(),
                    )
                })?;
                messages_out.inc_by(1);
                bytes_out.inc_by(bytes);
            }
            DataMeasurement::Collection { rows } => {
                if !self.collections_out {
                    return Err(ProcessorError::InvalidConfiguration(
                        "produced collection outside the planned output domain".to_string(),
                    ));
                }
                stats.record_collection_out(rows);
            }
            DataMeasurement::Other => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PassthroughMeasurement {
    Message,
    Collection { rows: u64 },
    Other,
}

/// Metrics for a passthrough boundary whose semantic domain is fixed by the physical plan.
///
/// A collection delivered by a collection-native sink is still one message at the sink tail.
/// Message-domain boundaries therefore classify payloads by the planned domain rather than by
/// their in-memory `StreamData` representation. Byte counters are intentionally omitted because
/// not every message representation exposes bytes.
pub(crate) struct PassthroughMetrics {
    domain: DataDomain,
    messages_in: Option<CounterHandle>,
    messages_out: Option<CounterHandle>,
}

impl PassthroughMetrics {
    pub(crate) fn new(stats: &ProcessorStats, specs: DataMetricSpecs, domain: DataDomain) -> Self {
        let (messages_in, messages_out) = match domain {
            DataDomain::Message => (
                Some(stats.register_counter(specs.messages_in)),
                Some(stats.register_counter(specs.messages_out)),
            ),
            DataDomain::Collection => {
                stats.declare_collection_in_out();
                (None, None)
            }
        };
        Self {
            domain,
            messages_in,
            messages_out,
        }
    }

    fn measure(&self, data: &StreamData) -> Result<PassthroughMeasurement, ProcessorError> {
        match (self.domain, data) {
            (DataDomain::Collection, StreamData::Collection(collection)) => {
                Ok(PassthroughMeasurement::Collection {
                    rows: collection.num_rows() as u64,
                })
            }
            (DataDomain::Collection, StreamData::Bytes(_))
            | (DataDomain::Collection, StreamData::EncodedDelivery { .. }) => {
                Err(ProcessorError::InvalidConfiguration(
                    "received message outside the planned collection domain".to_string(),
                ))
            }
            (DataDomain::Message, StreamData::Bytes(_))
            | (DataDomain::Message, StreamData::Collection(_)) => {
                Ok(PassthroughMeasurement::Message)
            }
            (DataDomain::Message, StreamData::EncodedDelivery { flags, bytes: _ })
                if flags.contains(crate::processor::EncodedDeliveryFlags::END)
                    && !flags.contains(crate::processor::EncodedDeliveryFlags::ABORT) =>
            {
                Ok(PassthroughMeasurement::Message)
            }
            (DataDomain::Message, StreamData::EncodedDelivery { .. }) => {
                Ok(PassthroughMeasurement::Other)
            }
            (_, StreamData::Control(_) | StreamData::Watermark(_) | StreamData::Error(_)) => {
                Ok(PassthroughMeasurement::Other)
            }
        }
    }

    pub(crate) fn record_input(
        &self,
        stats: &ProcessorStats,
        data: &StreamData,
    ) -> Result<PassthroughMeasurement, ProcessorError> {
        let measurement = self.measure(data)?;
        match measurement {
            PassthroughMeasurement::Message => self
                .messages_in
                .as_ref()
                .expect("message domain must declare messages_in")
                .inc_by(1),
            PassthroughMeasurement::Collection { rows } => stats.record_collection_in(rows),
            PassthroughMeasurement::Other => {}
        }
        Ok(measurement)
    }

    pub(crate) fn record_output(
        &self,
        stats: &ProcessorStats,
        measurement: PassthroughMeasurement,
    ) {
        match measurement {
            PassthroughMeasurement::Message => self
                .messages_out
                .as_ref()
                .expect("message domain must declare messages_out")
                .inc_by(1),
            PassthroughMeasurement::Collection { rows } => stats.record_collection_out(rows),
            PassthroughMeasurement::Other => {}
        }
    }
}
