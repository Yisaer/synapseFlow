use crate::codec::EncoderRegistry;
use crate::planner::physical::PhysicalStreamingEncoder;
use crate::planner::physical::{PhysicalDataSink, PhysicalPlan, PhysicalSinkConnector};
use std::sync::Arc;

/// A physical optimization rule.
trait PhysicalOptRule {
    fn name(&self) -> &str;
    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan>;
}

/// Apply physical plan optimizations using the provided registries.
pub fn optimize_physical_plan(
    physical_plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
) -> Arc<PhysicalPlan> {
    let rules: Vec<Box<dyn PhysicalOptRule>> = vec![Box::new(StreamingEncoderRewrite)];
    let mut current = physical_plan;
    for rule in rules {
        let _ = rule.name();
        current = rule.optimize(current, encoder_registry);
    }
    current
}

/// Rule: if a sink has a Batch -> Encoder chain and the encoder supports streaming,
/// rewrite it to StreamingEncoder -> Sink (dropping the batch).
struct StreamingEncoderRewrite;

impl PhysicalOptRule for StreamingEncoderRewrite {
    fn name(&self) -> &str {
        "streaming_encoder_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        self.optimize_node(plan, encoder_registry)
    }
}

impl StreamingEncoderRewrite {
    fn optimize_node(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        let optimized_children = self.optimize_children(plan.children(), encoder_registry);
        match plan.as_ref() {
            PhysicalPlan::DataSink(sink) => {
                self.optimize_data_sink(sink, optimized_children, encoder_registry)
            }
            _ => rebuild_with_children(plan.as_ref(), optimized_children),
        }
    }

    fn optimize_children(
        &self,
        children: &[Arc<PhysicalPlan>],
        encoder_registry: &EncoderRegistry,
    ) -> Vec<Arc<PhysicalPlan>> {
        children
            .iter()
            .map(|child| self.optimize_node(Arc::clone(child), encoder_registry))
            .collect()
    }

    fn optimize_data_sink(
        &self,
        sink: &PhysicalDataSink,
        optimized_children: Vec<Arc<PhysicalPlan>>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        if let Some(child) = optimized_children.first() {
            if let Some((rewritten_child, connector)) =
                self.rewrite_streaming_encoder_chain(sink, child, encoder_registry)
            {
                let mut new_sink = sink.clone();
                new_sink.base.children = vec![rewritten_child];
                new_sink.connector = connector;
                return Arc::new(PhysicalPlan::DataSink(new_sink));
            }
        }

        let mut new_sink = sink.clone();
        new_sink.base.children = optimized_children;
        Arc::new(PhysicalPlan::DataSink(new_sink))
    }

    fn rewrite_streaming_encoder_chain(
        &self,
        sink: &PhysicalDataSink,
        child: &Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Option<(Arc<PhysicalPlan>, PhysicalSinkConnector)> {
        let encoder = match child.as_ref() {
            PhysicalPlan::Encoder(encoder) => encoder,
            _ => return None,
        };

        if !encoder_registry.supports_streaming(encoder.encoder.kind()) {
            return None;
        }

        let batch = match encoder.base.children.first() {
            Some(plan) => match plan.as_ref() {
                PhysicalPlan::Batch(batch) => batch,
                _ => return None,
            },
            None => return None,
        };

        let upstream = batch.base.children.first().cloned()?;
        let streaming_index = encoder.base.index();
        let streaming_encoder = PhysicalStreamingEncoder::new(
            vec![upstream],
            streaming_index,
            encoder.sink_id.clone(),
            encoder.encoder.clone(),
            batch.common.clone(),
        );

        let mut connector = sink.connector.clone();
        connector.encoder_plan_index = streaming_index;

        Some((
            Arc::new(PhysicalPlan::StreamingEncoder(streaming_encoder)),
            connector,
        ))
    }
}

fn rebuild_with_children(
    plan: &PhysicalPlan,
    children: Vec<Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    match plan {
        PhysicalPlan::DataSource(ds) => {
            let mut new = ds.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::DataSource(new))
        }
        PhysicalPlan::SharedStream(stream) => {
            let mut new = stream.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SharedStream(new))
        }
        PhysicalPlan::Filter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Filter(new))
        }
        PhysicalPlan::Project(project) => {
            let mut new = project.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::Aggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Aggregation(new))
        }
        PhysicalPlan::Batch(batch) => {
            let mut new = batch.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Batch(new))
        }
        PhysicalPlan::Encoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Encoder(new))
        }
        PhysicalPlan::StreamingEncoder(streaming) => {
            let mut new = streaming.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StreamingEncoder(new))
        }
        PhysicalPlan::ResultCollect(collect) => {
            let mut new = collect.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ResultCollect(new))
        }
        PhysicalPlan::TumblingWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::TumblingWindow(new))
        }
        PhysicalPlan::CountWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::CountWindow(new))
        }
        PhysicalPlan::DataSink(sink) => {
            let mut new = sink.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::DataSink(new))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
    use crate::codec::{DecoderRegistry, EncoderRegistry};
    use crate::connector::ConnectorRegistry;
    use crate::planner::explain::PipelineExplain;
    use crate::planner::logical::create_logical_plan;
    use crate::planner::sink::{
        CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
        SinkEncoderConfig,
    };
    use crate::{AggregateFunctionRegistry, PipelineRegistries};
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use parser::parse_sql_with_registry;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn optimize_rewrites_batch_encoder_chain_to_streaming_encoder() {
        let encoder_registry = EncoderRegistry::with_builtin_encoders();
        let registries = PipelineRegistries::new(
            ConnectorRegistry::with_builtin_sinks(),
            Arc::clone(&encoder_registry),
            DecoderRegistry::with_builtin_decoders(),
            AggregateFunctionRegistry::with_builtins(),
        );

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "stream".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));
        let definition = StreamDefinition::new(
            "stream",
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::default()),
            StreamDecoderConfig::json(),
        );
        let mut stream_defs = HashMap::new();
        stream_defs.insert("stream".to_string(), Arc::new(definition));

        let select_stmt =
            parse_sql_with_registry("SELECT a FROM stream", registries.aggregate_registry())
                .expect("failed to parse SQL");
        let bindings = crate::expr::sql_conversion::SchemaBinding::new(vec![
            crate::expr::sql_conversion::SchemaBindingEntry {
                source_name: "stream".to_string(),
                alias: None,
                schema,
                kind: crate::expr::sql_conversion::SourceBindingKind::Regular,
            },
        ]);

        let mut common_props = CommonSinkProps::default();
        common_props.batch_count = Some(10);
        let connector = PipelineSinkConnector::new(
            "test_connector",
            SinkConnectorConfig::Nop(NopSinkConfig),
            SinkEncoderConfig::json(),
        );
        let sink = PipelineSink::new("test_sink", connector).with_common_props(common_props);

        let logical_plan =
            create_logical_plan(select_stmt, vec![sink], &stream_defs).expect("logical plan");
        let physical_plan =
            crate::planner::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
                .expect("physical plan");

        let pre_opt_explain =
            PipelineExplain::new(Arc::clone(&logical_plan), Arc::clone(&physical_plan));
        let pre_table = pre_opt_explain.physical.table_string();
        let expected_pre = r"- id                                 | info                                   
  PhysicalResultCollect_5            | sink_count=1                           
  └─PhysicalDataSink_2               | sink_id=test_sink, connector=nop       
    └─PhysicalEncoder_4              | sink_id=test_sink, encoder=json        
      └─PhysicalBatch_3              | sink_id=test_sink, batch_count=10      
        └─PhysicalProject_1          | fields=[a]                             
          └─PhysicalDataSource_0     | source=stream, decoder=json, schema=[a]";
        assert_eq!(pre_table.trim_end(), expected_pre);

        let optimized_plan =
            optimize_physical_plan(Arc::clone(&physical_plan), encoder_registry.as_ref());
        let post_explain = PipelineExplain::new(logical_plan, Arc::clone(&optimized_plan));
        let post_table = post_explain.physical.table_string();
        let expected_post = r"- id                                 | info                                          
  PhysicalResultCollect_5            | sink_count=1                                  
  └─PhysicalDataSink_2               | sink_id=test_sink, connector=nop              
    └─PhysicalStreamingEncoder_4     | sink_id=test_sink, encoder=json, batching=true
      └─PhysicalProject_1            | fields=[a]                                    
        └─PhysicalDataSource_0       | source=stream, decoder=json, schema=[a]";
        assert_eq!(post_table.trim_end(), expected_post);
    }
}
