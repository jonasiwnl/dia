use std::net::SocketAddr;

use clap::Parser;

use dia::sequencer::Sequencer;

// TODO: --background flag
// #[arg(short, long)]
// background: bool,
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// client | sequencer | repair
    service: String,

    /// IP + port for propose multicast
    #[arg(short, long, default_value_t = SocketAddr::from(([239, 0, 1, 1], 6000)))] 
    propose: SocketAddr,

    /// IP + port for consensus multicast
    #[arg(short, long, default_value_t = SocketAddr::from(([239, 0, 1, 2], 6000)))]
    consensus: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.service.as_str() {
        "client" => {
        },
        "sequencer" => {
            let sequencer = Sequencer::bind(args.propose, args.consensus).await?;
            sequencer.run().await?;
        },
        _ => {
            panic!("[main] unknown service type {}", args.service);
        },
    }

    Ok(())
}
