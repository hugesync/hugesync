//! HugeSync - A Cloud-Era Delta Synchronization Tool

use clap::Parser;
use hugesync::cli::{Cli, Commands, ConfigArgs};
use hugesync::config::Config;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    init_tracing(cli.verbose, cli.json);

    // Handle Ctrl+C gracefully
    let shutdown = setup_shutdown_handler();

    match cli.command {
        Commands::Sync(args) => {
            let config = args.to_config();
            tracing::info!(
                source = %args.source,
                destination = %args.destination,
                dry_run = config.dry_run,
                "Starting sync"
            );

            // TODO: Implement sync logic
            if config.dry_run {
                tracing::info!("Dry run mode - no changes will be made");
            }

            tracing::info!("Sync completed successfully");
        }

        Commands::Sign(args) => {
            tracing::info!(file = ?args.file, "Generating signature");

            // TODO: Implement signature generation
            let output = args
                .output
                .unwrap_or_else(|| args.file.with_extension("hssig"));

            tracing::info!(output = ?output, "Signature generated");
        }

        Commands::Config(args) => {
            handle_config_command(args)?;
        }
    }

    drop(shutdown);
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
