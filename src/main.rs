#[cfg(all(feature = "profiling", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use synapse_flow::server::{self, ServerOptions};

#[derive(Debug, Clone)]
struct CliFlags {
    profiling_enabled: Option<bool>,
    data_dir: Option<String>,
}

impl CliFlags {
    fn parse() -> Self {
        let mut profiling_enabled = None;
        let mut data_dir = None;
        let mut args = std::env::args().skip(1).peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--enable-profiling" | "--profiling" => profiling_enabled = Some(true),
                "--disable-profiling" | "--no-profiling" => profiling_enabled = Some(false),
                "--data-dir" => {
                    if let Some(val) = args.next() {
                        data_dir = Some(val);
                    }
                }
                _ => {}
            }
        }
        Self {
            profiling_enabled,
            data_dir,
        }
    }

    fn profiling_override(&self) -> Option<bool> {
        self.profiling_enabled
    }

    fn data_dir(&self) -> Option<&str> {
        self.data_dir.as_deref()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "[build_info] git_sha={} git_tag={}",
        build_info::git_sha(),
        build_info::git_tag()
    );

    let cli_flags = CliFlags::parse();
    let options = ServerOptions {
        profiling_enabled: cli_flags.profiling_override(),
        data_dir: cli_flags.data_dir().map(|s| s.to_string()),
        manager_addr: None,
    };

    // Prepare the FlowInstance so callers can register custom codecs/connectors.
    let instance = server::prepare_registry();
    // Inject custom registrations on `instance` here before starting, if needed.

    let ctx = server::init(options, instance).await?;
    server::start(ctx).await
}
