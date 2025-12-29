//! HugeSync - A Cloud-Era Delta Synchronization Tool

use clap::Parser;
use hugesync::cli::{Cli, Commands, ConfigArgs, SignArgs, SyncArgs};
use hugesync::config::Config;
use hugesync::signature;
use hugesync::sync::{create_backends, SyncEngine};
use hugesync::uri::Location;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    init_tracing(cli.verbose, cli.json);

    // Handle Ctrl+C gracefully
    let shutdown = setup_shutdown_handler();

    let result = match cli.command {
        Commands::Sync(args) => run_sync(args).await,
        Commands::Sign(args) => run_sign(args),
        Commands::Config(args) => handle_config_command(args),
    };

    drop(shutdown);
    result
}

async fn run_sync(args: SyncArgs) -> anyhow::Result<()> {
    let config = args.to_config();

    tracing::info!(
        source = %args.source,
        destination = %args.destination,
        dry_run = config.dry_run,
        jobs = config.effective_jobs(),
        block_size_kb = config.block_size_kb(),
        "Starting sync"
    );

    // Parse source and destination URIs
    let source_loc = Location::parse(&args.source)?;
    let dest_loc = Location::parse(&args.destination)?;

    tracing::debug!(
        source_type = source_loc.scheme(),
        dest_type = dest_loc.scheme(),
        "Parsed locations"
    );

    // Create storage backends
    let (source, dest) = create_backends(&source_loc, &dest_loc).await?;

    // Create and run sync engine
    let engine = SyncEngine::new(config, source, dest);
    let stats = engine.sync().await?;

    if stats.errors > 0 {
        tracing::warn!(errors = stats.errors, "Sync completed with errors");
        std::process::exit(1);
    }

    Ok(())
}

fn run_sign(args: SignArgs) -> anyhow::Result<()> {
    let block_size = hugesync::config::Config::validate_block_size(args.block_size as usize);

    tracing::info!(
        file = ?args.file,
        block_size_kb = block_size / 1024,
        "Generating signature"
    );

    // Generate signature
    let sig = signature::generate_signature(&args.file, block_size)?;

    // Determine output path
    let output = args
        .output
        .unwrap_or_else(|| args.file.with_extension("hssig"));

    // Write signature file
    signature::write_signature(&sig, &output)?;

    tracing::info!(
        output = ?output,
        blocks = sig.block_count(),
        file_size = sig.file_size,
        "Signature generated successfully"
    );

    println!(
        "Generated {} with {} blocks ({} bytes per block)",
        output.display(),
        sig.block_count(),
        sig.block_size
    );

    Ok(())
}

fn init_tracing(verbose: u8, json: bool) {
    let filter = match verbose {
        0 => EnvFilter::new("hugesync=info"),
        1 => EnvFilter::new("hugesync=debug"),
        2 => EnvFilter::new("hugesync=trace"),
        _ => EnvFilter::new("trace"),
    };

    if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().pretty())
            .init();
    }
}

fn setup_shutdown_handler() -> tokio::sync::oneshot::Sender<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("Received Ctrl+C, shutting down...");
                std::process::exit(130);
            }
            _ = rx => {
                // Normal shutdown
            }
        }
    });

    tx
}

fn handle_config_command(args: ConfigArgs) -> anyhow::Result<()> {
    if args.path {
        match Config::default_config_path() {
            Ok(path) => println!("{}", path.display()),
            Err(e) => eprintln!("Error: {}", e),
        }
    } else if args.init {
        let config = Config::default();
        config.save()?;
        println!(
            "Created default configuration at {}",
            Config::default_config_path()?.display()
        );
    } else {
        // Show current config
        let config = Config::load().unwrap_or_default();
        println!("{}", toml::to_string_pretty(&config)?);
    }
    Ok(())
}
