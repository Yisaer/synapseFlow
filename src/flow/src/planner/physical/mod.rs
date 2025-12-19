use std::sync::Arc;

pub mod base_physical;
pub mod physical_aggregation;
pub mod physical_batch;
pub mod physical_data_sink;
pub mod physical_data_source;
pub mod physical_decoder;
pub mod physical_encoder;
pub mod physical_filter;
pub mod physical_project;
pub mod physical_result_collect;
pub mod physical_shared_stream;
pub mod physical_streaming_aggregation;
pub mod physical_streaming_encoder;
pub mod physical_watermark;
pub mod physical_window;

pub use base_physical::BasePhysicalPlan;
pub use physical_aggregation::{AggregateCall, PhysicalAggregation};
pub use physical_batch::PhysicalBatch;
pub use physical_data_sink::{PhysicalDataSink, PhysicalSinkConnector};
pub use physical_data_source::PhysicalDataSource;
pub use physical_decoder::PhysicalDecoder;
pub use physical_encoder::PhysicalEncoder;
pub use physical_filter::PhysicalFilter;
pub use physical_project::{PhysicalProject, PhysicalProjectField};
pub use physical_result_collect::PhysicalResultCollect;
pub use physical_shared_stream::PhysicalSharedStream;
pub use physical_streaming_aggregation::{PhysicalStreamingAggregation, StreamingWindowSpec};
pub use physical_streaming_encoder::PhysicalStreamingEncoder;
pub use physical_watermark::{PhysicalWatermark, WatermarkConfig, WatermarkStrategy};
pub use physical_window::{
    PhysicalCountWindow, PhysicalSlidingWindow, PhysicalStateWindow, PhysicalTumblingWindow,
};

/// Enum describing all supported physical execution nodes
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    DataSource(PhysicalDataSource),
    Decoder(PhysicalDecoder),
    Filter(PhysicalFilter),
    Project(PhysicalProject),
    Aggregation(PhysicalAggregation),
    SharedStream(PhysicalSharedStream),
    Batch(PhysicalBatch),
    DataSink(PhysicalDataSink),
    Encoder(PhysicalEncoder),
    StreamingAggregation(PhysicalStreamingAggregation),
    StreamingEncoder(PhysicalStreamingEncoder),
    ResultCollect(PhysicalResultCollect),
    TumblingWindow(PhysicalTumblingWindow),
    CountWindow(PhysicalCountWindow),
    SlidingWindow(PhysicalSlidingWindow),
    StateWindow(Box<PhysicalStateWindow>),
    Watermark(PhysicalWatermark),
}

impl PhysicalPlan {
    /// Get the children of this physical plan
    pub fn children(&self) -> &[Arc<PhysicalPlan>] {
        match self {
            PhysicalPlan::DataSource(plan) => plan.base.children(),
            PhysicalPlan::Decoder(plan) => plan.base.children(),
            PhysicalPlan::Filter(plan) => plan.base.children(),
            PhysicalPlan::Project(plan) => plan.base.children(),
            PhysicalPlan::Aggregation(plan) => plan.base.children(),
            PhysicalPlan::SharedStream(plan) => plan.base.children(),
            PhysicalPlan::Batch(plan) => plan.base.children(),
            PhysicalPlan::DataSink(plan) => plan.base.children(),
            PhysicalPlan::Encoder(plan) => plan.base.children(),
            PhysicalPlan::StreamingAggregation(plan) => plan.base.children(),
            PhysicalPlan::StreamingEncoder(plan) => plan.base.children(),
            PhysicalPlan::ResultCollect(plan) => plan.base.children(),
            PhysicalPlan::TumblingWindow(plan) => plan.base.children(),
            PhysicalPlan::CountWindow(plan) => plan.base.children(),
            PhysicalPlan::SlidingWindow(plan) => plan.base.children(),
            PhysicalPlan::StateWindow(plan) => plan.base.children(),
            PhysicalPlan::Watermark(plan) => plan.base.children(),
        }
    }

    /// Get the type name of this physical plan
    pub fn get_plan_type(&self) -> &str {
        match self {
            PhysicalPlan::DataSource(_) => "PhysicalDataSource",
            PhysicalPlan::Decoder(_) => "PhysicalDecoder",
            PhysicalPlan::Filter(_) => "PhysicalFilter",
            PhysicalPlan::Project(_) => "PhysicalProject",
            PhysicalPlan::Aggregation(_) => "PhysicalAggregation",
            PhysicalPlan::SharedStream(_) => "PhysicalSharedStream",
            PhysicalPlan::Batch(_) => "PhysicalBatch",
            PhysicalPlan::DataSink(_) => "PhysicalDataSink",
            PhysicalPlan::Encoder(_) => "PhysicalEncoder",
            PhysicalPlan::StreamingAggregation(_) => "PhysicalStreamingAggregation",
            PhysicalPlan::StreamingEncoder(_) => "PhysicalStreamingEncoder",
            PhysicalPlan::ResultCollect(_) => "PhysicalResultCollect",
            PhysicalPlan::TumblingWindow(_) => "PhysicalTumblingWindow",
            PhysicalPlan::CountWindow(_) => "PhysicalCountWindow",
            PhysicalPlan::SlidingWindow(_) => "PhysicalSlidingWindow",
            PhysicalPlan::StateWindow(_) => "PhysicalStateWindow",
            PhysicalPlan::Watermark(_) => "PhysicalWatermark",
        }
    }

    /// Get the unique index of this physical plan
    pub fn get_plan_index(&self) -> i64 {
        match self {
            PhysicalPlan::DataSource(plan) => plan.base.index(),
            PhysicalPlan::Decoder(plan) => plan.base.index(),
            PhysicalPlan::Filter(plan) => plan.base.index(),
            PhysicalPlan::Project(plan) => plan.base.index(),
            PhysicalPlan::Aggregation(plan) => plan.base.index(),
            PhysicalPlan::SharedStream(plan) => plan.base.index(),
            PhysicalPlan::Batch(plan) => plan.base.index(),
            PhysicalPlan::DataSink(plan) => plan.base.index(),
            PhysicalPlan::Encoder(plan) => plan.base.index(),
            PhysicalPlan::StreamingAggregation(plan) => plan.base.index(),
            PhysicalPlan::StreamingEncoder(plan) => plan.base.index(),
            PhysicalPlan::ResultCollect(plan) => plan.base.index(),
            PhysicalPlan::TumblingWindow(plan) => plan.base.index(),
            PhysicalPlan::CountWindow(plan) => plan.base.index(),
            PhysicalPlan::SlidingWindow(plan) => plan.base.index(),
            PhysicalPlan::StateWindow(plan) => plan.base.index(),
            PhysicalPlan::Watermark(plan) => plan.base.index(),
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
