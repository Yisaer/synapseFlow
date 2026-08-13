use std::sync::Arc;

pub mod base_physical;
pub mod output_layout;
pub mod physical_aggregation;
pub mod physical_barrier;
pub mod physical_batch;
pub mod physical_collection_layout_normalize;
pub mod physical_column_filter;
pub mod physical_compute;
pub mod physical_data_sink;
pub mod physical_data_source;
pub mod physical_decoder;
pub mod physical_empty_suppress;
pub mod physical_eventtime_watermark;
pub mod physical_filter;
pub mod physical_inc_sink_encoder;
pub mod physical_memory_collection_materialize;
pub mod physical_order;
pub mod physical_process_time_watermark;
pub mod physical_project;
pub mod physical_result_collect;
pub mod physical_row_diff;
pub mod physical_sampler;
pub mod physical_shared_stream;
pub mod physical_sink_compress;
pub mod physical_sink_encoder;
pub mod physical_sink_encrypt;
pub mod physical_source_change_gate;
pub mod physical_stateful_function;
pub mod physical_streaming_aggregation;
pub mod physical_table_scan;
pub mod physical_watermark;
pub mod physical_window;
pub mod pipeline_state_usage;

pub use base_physical::BasePhysicalPlan;
pub use physical_aggregation::{AggregateCall, PhysicalAggregation};
pub use physical_barrier::PhysicalBarrier;
pub use physical_batch::PhysicalBatch;
pub use physical_collection_layout_normalize::PhysicalCollectionLayoutNormalize;
pub use physical_column_filter::PhysicalColumnFilter;
pub use physical_compute::{PhysicalCompute, PhysicalComputeField};
pub use physical_data_sink::{PhysicalDataSink, PhysicalSinkConnector};
pub use physical_data_source::PhysicalDataSource;
pub use physical_decoder::PhysicalDecoder;
pub use physical_decoder::PhysicalDecoderEventtimeSpec;
pub use physical_empty_suppress::PhysicalEmptySuppress;
pub use physical_eventtime_watermark::PhysicalEventtimeWatermark;
pub use physical_filter::PhysicalFilter;
pub use physical_inc_sink_encoder::PhysicalIncSinkEncoder;
pub use physical_memory_collection_materialize::PhysicalMemoryCollectionMaterialize;
pub use physical_order::{PhysicalOrder, PhysicalOrderKey};
pub use physical_process_time_watermark::PhysicalProcessTimeWatermark;
pub use physical_project::{PhysicalProject, PhysicalProjectField};
pub use physical_result_collect::PhysicalResultCollect;
pub use physical_row_diff::PhysicalRowDiff;
pub use physical_sampler::PhysicalSampler;
pub use physical_shared_stream::{PhysicalSharedStream, PhysicalSharedStreamRequirement};
pub use physical_sink_compress::PhysicalSinkCompress;
pub use physical_sink_encoder::PhysicalSinkEncoder;
pub use physical_sink_encrypt::PhysicalSinkEncrypt;
pub use physical_source_change_gate::PhysicalSourceChangeGate;
pub use physical_stateful_function::{PartitionGroupKey, PhysicalStatefulFunction, StatefulCall};
pub use physical_streaming_aggregation::{PhysicalStreamingAggregation, StreamingWindowSpec};
pub use physical_table_scan::{PhysicalTableScan, PhysicalTableScanSpec};
pub use physical_watermark::{PhysicalWatermark, WatermarkConfig};
pub use physical_window::{
    PhysicalCountWindow, PhysicalEosWindow, PhysicalSlidingWindow, PhysicalStateWindow,
    PhysicalTumblingWindow,
};
pub use pipeline_state_usage::PipelineStateUsage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataDomain {
    Message,
    Collection,
}

/// Enum describing all supported physical execution nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    DataSource(PhysicalDataSource),
    TableScan(PhysicalTableScan),
    Decoder(PhysicalDecoder),
    CollectionLayoutNormalize(PhysicalCollectionLayoutNormalize),
    MemoryCollectionMaterialize(PhysicalMemoryCollectionMaterialize),
    StatefulFunction(PhysicalStatefulFunction),
    Filter(PhysicalFilter),
    Compute(PhysicalCompute),
    Order(PhysicalOrder),
    Project(PhysicalProject),
    RowDiff(PhysicalRowDiff),
    ColumnFilter(PhysicalColumnFilter),
    EmptySuppress(PhysicalEmptySuppress),
    Aggregation(PhysicalAggregation),
    SharedStream(PhysicalSharedStream),
    SourceChangeGate(PhysicalSourceChangeGate),
    Batch(PhysicalBatch),
    DataSink(PhysicalDataSink),
    SinkConnector(PhysicalDataSink),
    SinkEncoder(PhysicalSinkEncoder),

    IncSinkEncoder(PhysicalIncSinkEncoder),
    SinkCompress(PhysicalSinkCompress),
    SinkEncrypt(PhysicalSinkEncrypt),

    StreamingAggregation(PhysicalStreamingAggregation),
    ResultCollect(PhysicalResultCollect),
    Barrier(PhysicalBarrier),
    TumblingWindow(PhysicalTumblingWindow),
    CountWindow(PhysicalCountWindow),
    SlidingWindow(PhysicalSlidingWindow),
    StateWindow(Box<PhysicalStateWindow>),
    EosWindow(PhysicalEosWindow),
    /// Processing-time watermark physical node (ticker-based).
    ProcessTimeWatermark(PhysicalProcessTimeWatermark),
    /// Event-time watermark physical node (data-driven).
    EventtimeWatermark(PhysicalEventtimeWatermark),
    /// Back-compat (deprecated).
    Watermark(PhysicalWatermark),
    /// Sampler for stream downsampling.
    Sampler(PhysicalSampler),
}

impl PhysicalPlan {
    /// Get the children of this physical plan
    pub fn children(&self) -> &[Arc<PhysicalPlan>] {
        match self {
            PhysicalPlan::DataSource(plan) => plan.base.children(),
            PhysicalPlan::TableScan(plan) => plan.base.children(),
            PhysicalPlan::Decoder(plan) => plan.base.children(),
            PhysicalPlan::CollectionLayoutNormalize(plan) => plan.base.children(),
            PhysicalPlan::MemoryCollectionMaterialize(plan) => plan.base.children(),
            PhysicalPlan::StatefulFunction(plan) => plan.base.children(),
            PhysicalPlan::Filter(plan) => plan.base.children(),
            PhysicalPlan::Compute(plan) => plan.base.children(),
            PhysicalPlan::Order(plan) => plan.base.children(),
            PhysicalPlan::Project(plan) => plan.base.children(),
            PhysicalPlan::RowDiff(plan) => plan.base.children(),
            PhysicalPlan::ColumnFilter(plan) => plan.base.children(),
            PhysicalPlan::EmptySuppress(plan) => plan.base.children(),
            PhysicalPlan::Aggregation(plan) => plan.base.children(),
            PhysicalPlan::SharedStream(plan) => plan.base.children(),
            PhysicalPlan::SourceChangeGate(plan) => plan.base.children(),
            PhysicalPlan::Batch(plan) => plan.base.children(),
            PhysicalPlan::DataSink(plan) | PhysicalPlan::SinkConnector(plan) => {
                plan.base.children()
            }
            PhysicalPlan::SinkEncoder(plan) => plan.base.children(),

            PhysicalPlan::IncSinkEncoder(plan) => plan.base.children(),
            PhysicalPlan::SinkCompress(plan) => plan.base.children(),
            PhysicalPlan::SinkEncrypt(plan) => plan.base.children(),

            PhysicalPlan::StreamingAggregation(plan) => plan.base.children(),
            PhysicalPlan::ResultCollect(plan) => plan.base.children(),
            PhysicalPlan::Barrier(plan) => plan.base.children(),
            PhysicalPlan::TumblingWindow(plan) => plan.base.children(),
            PhysicalPlan::CountWindow(plan) => plan.base.children(),
            PhysicalPlan::SlidingWindow(plan) => plan.base.children(),
            PhysicalPlan::StateWindow(plan) => plan.base.children(),
            PhysicalPlan::EosWindow(plan) => plan.base.children(),
            PhysicalPlan::ProcessTimeWatermark(plan) => plan.base.children(),
            PhysicalPlan::EventtimeWatermark(plan) => plan.base.children(),
            PhysicalPlan::Watermark(plan) => plan.base.children(),
            PhysicalPlan::Sampler(plan) => plan.base.children(),
        }
    }

    /// Get the type name of this physical plan
    pub fn get_plan_type(&self) -> &str {
        match self {
            PhysicalPlan::DataSource(_) => "PhysicalDataSource",
            PhysicalPlan::TableScan(_) => "PhysicalTableScan",
            PhysicalPlan::Decoder(_) => "PhysicalDecoder",
            PhysicalPlan::CollectionLayoutNormalize(_) => "PhysicalCollectionLayoutNormalize",
            PhysicalPlan::MemoryCollectionMaterialize(_) => "PhysicalMemoryCollectionMaterialize",
            PhysicalPlan::StatefulFunction(_) => "PhysicalStatefulFunction",
            PhysicalPlan::Filter(_) => "PhysicalFilter",
            PhysicalPlan::Compute(_) => "PhysicalCompute",
            PhysicalPlan::Order(_) => "PhysicalOrder",
            PhysicalPlan::Project(_) => "PhysicalProject",
            PhysicalPlan::RowDiff(_) => "PhysicalRowDiff",
            PhysicalPlan::ColumnFilter(_) => "PhysicalColumnFilter",
            PhysicalPlan::EmptySuppress(_) => "PhysicalEmptySuppress",
            PhysicalPlan::Aggregation(_) => "PhysicalAggregation",
            PhysicalPlan::SharedStream(_) => "PhysicalSharedStream",
            PhysicalPlan::SourceChangeGate(_) => "PhysicalSourceChangeGate",
            PhysicalPlan::Batch(_) => "PhysicalBatch",
            PhysicalPlan::DataSink(_) => "PhysicalDataSink",
            PhysicalPlan::SinkConnector(_) => "PhysicalSinkConnector",
            PhysicalPlan::SinkEncoder(_) => "PhysicalSinkEncoder",

            PhysicalPlan::IncSinkEncoder(_) => "PhysicalIncSinkEncoder",
            PhysicalPlan::SinkCompress(_) => "PhysicalSinkCompress",
            PhysicalPlan::SinkEncrypt(_) => "PhysicalSinkEncrypt",

            PhysicalPlan::StreamingAggregation(_) => "PhysicalStreamingAggregation",
            PhysicalPlan::ResultCollect(_) => "PhysicalResultCollect",
            PhysicalPlan::Barrier(_) => "PhysicalBarrier",
            PhysicalPlan::TumblingWindow(_) => "PhysicalTumblingWindow",
            PhysicalPlan::CountWindow(_) => "PhysicalCountWindow",
            PhysicalPlan::SlidingWindow(_) => "PhysicalSlidingWindow",
            PhysicalPlan::StateWindow(_) => "PhysicalStateWindow",
            PhysicalPlan::EosWindow(_) => "PhysicalEosWindow",
            PhysicalPlan::ProcessTimeWatermark(_) => "PhysicalProcessTimeWatermark",
            PhysicalPlan::EventtimeWatermark(_) => "PhysicalEventtimeWatermark",
            PhysicalPlan::Watermark(_) => "PhysicalWatermark",
            PhysicalPlan::Sampler(_) => "PhysicalSampler",
        }
    }

    /// Get the unique index of this physical plan
    pub fn get_plan_index(&self) -> i64 {
        match self {
            PhysicalPlan::DataSource(plan) => plan.base.index(),
            PhysicalPlan::TableScan(plan) => plan.base.index(),
            PhysicalPlan::Decoder(plan) => plan.base.index(),
            PhysicalPlan::CollectionLayoutNormalize(plan) => plan.base.index(),
            PhysicalPlan::MemoryCollectionMaterialize(plan) => plan.base.index(),
            PhysicalPlan::StatefulFunction(plan) => plan.base.index(),
            PhysicalPlan::Filter(plan) => plan.base.index(),
            PhysicalPlan::Compute(plan) => plan.base.index(),
            PhysicalPlan::Order(plan) => plan.base.index(),
            PhysicalPlan::Project(plan) => plan.base.index(),
            PhysicalPlan::RowDiff(plan) => plan.base.index(),
            PhysicalPlan::ColumnFilter(plan) => plan.base.index(),
            PhysicalPlan::EmptySuppress(plan) => plan.base.index(),
            PhysicalPlan::Aggregation(plan) => plan.base.index(),
            PhysicalPlan::SharedStream(plan) => plan.base.index(),
            PhysicalPlan::SourceChangeGate(plan) => plan.base.index(),
            PhysicalPlan::Batch(plan) => plan.base.index(),
            PhysicalPlan::DataSink(plan) | PhysicalPlan::SinkConnector(plan) => plan.base.index(),
            PhysicalPlan::SinkEncoder(plan) => plan.base.index(),

            PhysicalPlan::IncSinkEncoder(plan) => plan.base.index(),
            PhysicalPlan::SinkCompress(plan) => plan.base.index(),
            PhysicalPlan::SinkEncrypt(plan) => plan.base.index(),

            PhysicalPlan::StreamingAggregation(plan) => plan.base.index(),
            PhysicalPlan::ResultCollect(plan) => plan.base.index(),
            PhysicalPlan::Barrier(plan) => plan.base.index(),
            PhysicalPlan::TumblingWindow(plan) => plan.base.index(),
            PhysicalPlan::CountWindow(plan) => plan.base.index(),
            PhysicalPlan::SlidingWindow(plan) => plan.base.index(),
            PhysicalPlan::StateWindow(plan) => plan.base.index(),
            PhysicalPlan::EosWindow(plan) => plan.base.index(),
            PhysicalPlan::ProcessTimeWatermark(plan) => plan.base.index(),
            PhysicalPlan::EventtimeWatermark(plan) => plan.base.index(),
            PhysicalPlan::Watermark(plan) => plan.base.index(),
            PhysicalPlan::Sampler(plan) => plan.base.index(),
        }
    }

    /// Get the plan name in format: {{plan_type}}_{{plan_index}}
    pub fn get_plan_name(&self) -> String {
        format!("{}_{}", self.get_plan_type(), self.get_plan_index())
    }

    /// Print the topology structure of this physical plan for debugging
    pub fn print_topology(&self, indent: usize) {
        let spacing = "  ".repeat(indent);
        println!(
            "{}{} (index: {})",
            spacing,
            self.get_plan_type(),
            self.get_plan_index()
        );

        for child in self.children() {
            child.print_topology(indent + 1);
        }
    }
}
