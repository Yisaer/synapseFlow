//! Runtime checkpoint barrier injection.
//!
//! This module owns the pipeline control-plane operation of injecting a
//! checkpoint barrier and waiting for the tail acknowledgement. The enclosing
//! processor pipeline commits the manifest after the acknowledgement; snapshot
//! participants can be added without changing this barrier protocol.

use crate::checkpoint::CheckpointMode;
use crate::processor::result_collect_processor::AckManager;
use crate::processor::{Ingress, ProcessorError, StreamData};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[derive(Clone)]
pub(crate) struct CheckpointTrigger {
    ingress: mpsc::Sender<Ingress>,
    ack_manager: Arc<AckManager>,
    signal_id_allocator: Arc<AtomicU64>,
}

impl CheckpointTrigger {
    pub(crate) fn new(
        ingress: mpsc::Sender<Ingress>,
        ack_manager: Arc<AckManager>,
        signal_id_allocator: Arc<AtomicU64>,
    ) -> Self {
        Self {
            ingress,
            ack_manager,
            signal_id_allocator,
        }
    }

    pub(crate) async fn inject_and_wait(
        &self,
        mode: CheckpointMode,
        timeout_duration: Duration,
    ) -> Result<u64, ProcessorError> {
        let checkpoint_id = self.signal_id_allocator.fetch_add(1, Ordering::Relaxed);
        let receiver = self.ack_manager.register(checkpoint_id)?;
        let signal = StreamData::checkpoint(checkpoint_id, mode);

        if self.ingress.send(Ingress::data(signal)).await.is_err() {
            self.ack_manager.unregister(checkpoint_id);
            return Err(ProcessorError::ChannelClosed);
        }

        let acknowledged = match timeout(timeout_duration, receiver).await {
            Ok(Ok(signal)) => signal,
            Ok(Err(_)) => {
                self.ack_manager.unregister(checkpoint_id);
                return Err(ProcessorError::ChannelClosed);
            }
            Err(_) => {
                self.ack_manager.unregister(checkpoint_id);
                return Err(ProcessorError::Timeout);
            }
        };

        if acknowledged.id() != checkpoint_id || acknowledged.checkpoint_mode() != Some(mode) {
            return Err(ProcessorError::ProcessingError(format!(
                "unexpected checkpoint acknowledgement for checkpoint_id={checkpoint_id}"
            )));
        }

        Ok(checkpoint_id)
    }
}

#[derive(Clone)]
pub(crate) struct CheckpointCoordinator {
    trigger: CheckpointTrigger,
}

impl CheckpointCoordinator {
    pub(crate) fn new(trigger: CheckpointTrigger) -> Self {
        Self { trigger }
    }

    pub(crate) async fn request_checkpoint(
        &self,
        timeout_duration: Duration,
    ) -> Result<u64, ProcessorError> {
        self.trigger
            .inject_and_wait(CheckpointMode::Continue, timeout_duration)
            .await
    }

    pub(crate) async fn request_final_checkpoint(
        &self,
        timeout_duration: Duration,
    ) -> Result<u64, ProcessorError> {
        self.trigger
            .inject_and_wait(CheckpointMode::Final, timeout_duration)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::base::Processor;
    use crate::processor::result_collect_processor::AckHook;
    use crate::processor::{
        BarrierProcessor, ControlSignal, ControlSourceProcessor, IngressTarget,
        ResultCollectProcessor,
    };
    use crate::runtime::TaskSpawner;
    use std::time::Duration;

    fn test_trigger() -> (CheckpointTrigger, mpsc::Receiver<Ingress>, Arc<AckManager>) {
        let (ingress, receiver) = mpsc::channel(1);
        let ack_manager = Arc::new(AckManager::default());
        let trigger = CheckpointTrigger::new(
            ingress,
            Arc::clone(&ack_manager),
            Arc::new(AtomicU64::new(1)),
        );
        (trigger, receiver, ack_manager)
    }

    #[tokio::test]
    async fn injects_checkpoint_on_data_ingress_and_waits_for_tail_ack() {
        let (trigger, mut ingress, ack_manager) = test_trigger();
        let request = tokio::spawn(async move {
            trigger
                .inject_and_wait(CheckpointMode::Continue, Duration::from_secs(1))
                .await
        });

        let item = ingress.recv().await.expect("checkpoint ingress");
        assert_eq!(item.target, IngressTarget::Data);
        let StreamData::Control(signal) = item.data else {
            panic!("checkpoint must be a control signal on the data channel");
        };
        assert_eq!(signal.checkpoint_mode(), Some(CheckpointMode::Continue));
        ack_manager.ack(&signal);

        assert_eq!(request.await.expect("checkpoint request task"), Ok(1));
    }

    #[tokio::test]
    async fn final_checkpoint_is_terminal() {
        let (trigger, mut ingress, ack_manager) = test_trigger();
        let coordinator = CheckpointCoordinator::new(trigger);
        let request = tokio::spawn(async move {
            coordinator
                .request_final_checkpoint(Duration::from_secs(1))
                .await
        });

        let item = ingress.recv().await.expect("final checkpoint ingress");
        let StreamData::Control(signal) = item.data else {
            panic!("final checkpoint must be a control signal");
        };
        assert!(signal.is_terminal());
        assert_eq!(signal.checkpoint_mode(), Some(CheckpointMode::Final));
        ack_manager.ack(&signal);

        assert_eq!(request.await.expect("final checkpoint request task"), Ok(1));
    }

    #[tokio::test]
    async fn returns_timeout_when_tail_does_not_ack() {
        let (trigger, mut ingress, _ack_manager) = test_trigger();
        let request = tokio::spawn(async move {
            trigger
                .inject_and_wait(CheckpointMode::Continue, Duration::from_millis(1))
                .await
        });
        let _ = ingress.recv().await.expect("checkpoint ingress");

        assert_eq!(
            request.await.expect("checkpoint request task"),
            Err(ProcessorError::Timeout)
        );
    }

    #[tokio::test]
    async fn checkpoint_barrier_passes_through_data_pipeline_to_tail() {
        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
        let channel_capacities = crate::processor::base::default_channel_capacities();
        let mut source =
            ControlSourceProcessor::new_with_channel_capacities("test_source", channel_capacities);
        let (ingress, ingress_receiver) = mpsc::channel(channel_capacities.control);
        source.set_ingress_input(ingress_receiver);

        let mut barrier =
            BarrierProcessor::new_with_channel_capacities("test_barrier", 1, channel_capacities);
        barrier.add_input(source.subscribe_output().expect("source data output"));
        barrier.add_control_input(
            source
                .subscribe_control_output()
                .expect("source control output"),
        );

        let mut tail = ResultCollectProcessor::new("test_tail");
        tail.add_input(barrier.subscribe_output().expect("barrier data output"));
        tail.add_control_input(
            barrier
                .subscribe_control_output()
                .expect("barrier control output"),
        );
        let (tail_output, mut output) = mpsc::channel(channel_capacities.control);
        tail.set_output(tail_output);

        let ack_manager = Arc::new(AckManager::default());
        tail.add_bus_hook(Arc::new(AckHook::new(Arc::clone(&ack_manager))));
        let coordinator = CheckpointCoordinator::new(CheckpointTrigger::new(
            ingress,
            Arc::clone(&ack_manager),
            source.control_signal_id_allocator(),
        ));

        let source_handle = source.start(&spawner).handle;
        let barrier_handle = barrier.start(&spawner).handle;
        let tail_handle = tail.start(&spawner).handle;

        let checkpoint_id = coordinator
            .request_checkpoint(Duration::from_secs(1))
            .await
            .expect("continue checkpoint");
        let checkpoint = timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("continue checkpoint output timeout")
            .expect("continue checkpoint output closed");
        assert_eq!(
            checkpoint
                .as_control()
                .and_then(ControlSignal::checkpoint_mode),
            Some(CheckpointMode::Continue)
        );
        assert_eq!(
            checkpoint.as_control().map(ControlSignal::id),
            Some(checkpoint_id)
        );

        let final_id = coordinator
            .request_final_checkpoint(Duration::from_secs(1))
            .await
            .expect("final checkpoint");
        let final_checkpoint = timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("final checkpoint output timeout")
            .expect("final checkpoint output closed");
        assert!(final_checkpoint
            .as_control()
            .is_some_and(ControlSignal::is_terminal));
        assert_eq!(
            final_checkpoint
                .as_control()
                .and_then(ControlSignal::checkpoint_mode),
            Some(CheckpointMode::Final)
        );
        assert_eq!(
            final_checkpoint.as_control().map(ControlSignal::id),
            Some(final_id)
        );

        assert!(source_handle.await.expect("source task join").is_ok());
        assert!(barrier_handle.await.expect("barrier task join").is_ok());
        assert!(tail_handle.await.expect("tail task join").is_ok());
    }
}
