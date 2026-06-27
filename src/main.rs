use clap::Parser;

use dia::sequencer::Sequencer;

// TODO: --background arg
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    service: String,

    // How many of the service to start
    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // TODO: make args
    let propose_addr = "239.0.1.1:6000".parse().unwrap();
    let consensus_addr = "239.0.1.2:6000".parse().unwrap();

    match args.service.as_str() {
        "client" => {},
        "sequencer" => {
            let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
            sequencer.run().await?;
        },
        _ => {
            panic!("[main] unknown service type {}", args.service);
        },
    }

    Ok(())
}
