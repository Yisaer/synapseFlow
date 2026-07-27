use crate::catalog::StreamDecoderConfig;
use crate::planner::physical::{
    PhysicalDataSource, PhysicalDecoder, PhysicalPlan, PhysicalResultCollect, PhysicalSampler,
};
use crate::processor::SamplerConfig;
use datatypes::Schema;
use std::sync::Arc;

struct IndexCounter {
    next_index: i64,
}

impl IndexCounter {
    fn new(start_index: i64) -> Self {
        Self {
            next_index: start_index,
        }
    }

    fn allocate(&mut self) -> i64 {
        let idx = self.next_index;
        self.next_index += 1;
        idx
    }
}

pub(crate) fn create_physical_plan_for_shared_stream(
    stream_name: &str,
    schema: Arc<Schema>,
    decoder: StreamDecoderConfig,
    sampler: Option<SamplerConfig>,
) -> Arc<PhysicalPlan> {
    let mut index_counter = IndexCounter::new(0);

    let datasource_plan = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
        stream_name.to_string(),
        Arc::clone(&schema),
        None,
        index_counter.allocate(),
    )));

    let sampler_input = Arc::clone(&datasource_plan);
    let sampler_plan = sampler.map(|config| {
        Arc::new(PhysicalPlan::Sampler(PhysicalSampler::new(
            config.interval,
            config.strategy,
            decoder.schema_artifact.clone(),
            vec![sampler_input],
            index_counter.allocate(),
        )))
    });

    let decoder_children = sampler_plan
        .as_ref()
        .map(|plan| vec![Arc::clone(plan)])
        .unwrap_or_else(|| vec![Arc::clone(&datasource_plan)]);
    let decoder_plan = Arc::new(PhysicalPlan::Decoder(PhysicalDecoder::new(
        stream_name.to_string(),
        decoder,
        Arc::clone(&schema),
        None,
        None,
        decoder_children,
        index_counter.allocate(),
    )));

    Arc::new(PhysicalPlan::ResultCollect(PhysicalResultCollect::new(
        vec![Arc::clone(&decoder_plan)],
        index_counter.allocate(),
    )))
}
