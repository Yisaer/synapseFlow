#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

#[cfg(all(feature = "allocator-jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use veloflux::server;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `veloflux secrets ...` runs the local secret-store CLI and exits without
    // starting the server (VF-51 §6.1.3).
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("secrets") {
        return veloflux::secrets_cli::run(&args);
    }

    let bootstrap = veloflux::bootstrap::default_init_options()?;
    let instance = server::prepare_registry(&bootstrap.options.flow_instances)?;
    let _logging_guard = bootstrap.logging_guard;
    veloflux::distro::register_selected_distro(&instance);
    let ctx = server::init(bootstrap.options, instance).await?;
    server::start(ctx).await
}
