use std::{io::BufRead, net::SocketAddr};

use clap::Parser;
use serde::{Deserialize, Serialize};

use dia::{client::Client, sequencer::Sequencer};

#[derive(Serialize, Deserialize)]
struct TerminalMessage {
    data: String,
}

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
            let (sender, receiver) =
                Client::<TerminalMessage>::bind_split(args.propose, args.consensus).await?;
            tokio::spawn(async move {
                if let Err(e) = receiver
                    .listen(|message| println!("{}", message.payload.data))
                    .await
                {
                    eprintln!("[client] listener stopped: {:?}", e);
                }
            });

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            std::thread::spawn(move || {
                let stdin = std::io::stdin();
                for line in stdin.lock().lines() {
                    match line {
                        Ok(line) => {
                            if tx.send(line).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("[client] stdin read error: {:?}", e);
                            break;
                        }
                    }
                }
            });

            while let Some(line) = rx.recv().await {
                sender.send(TerminalMessage { data: line }).await?;
            }
        }
        "sequencer" => {
            let sequencer = Sequencer::bind(args.propose, args.consensus).await?;
            sequencer.run().await?;
        }
        "repair" => {
            anyhow::bail!("[main] repair has not been implemented yet");
        }
        _ => {
            anyhow::bail!("[main] unknown service type {}", args.service);
        }
    }

    Ok(())
}
