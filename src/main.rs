#[cfg(all(feature = "profiling", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use synapse_flow::server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = synapse_flow::bootstrap::default_init()?;
    // Keep logging guard alive for the duration of the application
    let _logging_guard = result.logging_guard;

    let ctx = server::init(result.options, result.instance).await?;
    server::start(ctx).await
}
