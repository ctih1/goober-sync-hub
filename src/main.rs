use std::{collections::{HashMap, HashSet}, hash::{Hash, Hasher}, net::{IpAddr, SocketAddr}, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use futures::{SinkExt, StreamExt};
use log::{error, info, debug, warn};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use fxhash::FxHasher;

type EventKey = String;

#[derive(Debug)]
struct Bot {
    associated_ip: SocketAddr,
    accessible_events: HashSet<EventKey>,
    event_counts: HashMap<EventKey, i32>,
    channel_ids: HashSet<u64>
}

type BotMap = Arc<Mutex<HashMap<String, Bot>>>;
type HandledEvents = Arc<Mutex<HashSet<u64>>>;

#[tokio::main]
async fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Debug).init();
    
    let address = "0.0.0.0:3960";
    let addr: SocketAddr =  address.parse().expect("Failed to parse address");
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");

    let bot_map: BotMap = Arc::new(Mutex::new(HashMap::new()));
    let handled_events: HandledEvents = Arc::new(Mutex::new(HashSet::new()));

    info!("Listening on {}", address);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, Arc::clone(&bot_map), Arc::clone(&handled_events)));
    }
}

async fn handle_connection(stream: TcpStream, bot_map_mutex: BotMap, handled_events_mutex: HandledEvents) {
    let peer_addr = &stream.peer_addr().unwrap();
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("Error handshaking conn {}", e);
            return;
        }
    };

    let (mut sender, mut receiver) = ws_stream.split();

    info!("New connection");

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let texts: Vec<&str> = text.split(";").collect();

                let mut event: String = String::new();
                let mut message_id: u64 = 1010;
                let mut channel_id: u64 = 1010;
                let mut bot_name: String = String::new();

                for arg in texts {
                    let parts: Vec<&str> = arg.split("=").collect();
                    
                    if parts.len() != 2 {
                        let res = sender.send(Message::Text(format!("Invalid body; expected 2 parts for arg {}", arg).to_string())).await;
                        if let Err(_) = res {
                            error!("Failed to send invalid request body");
                            let _ = sender.close().await;
                        }
                        return;
                    }
                    
                    let [key, value] = parts.as_slice().try_into().unwrap();

                    match key {
                        "ref" => { message_id = value.parse().unwrap_or(1010); }
                        "event" => { event = value.to_string(); }
                        "name" => { bot_name = value.to_string(); }
                        "channel" => { channel_id = value.parse().unwrap_or(1010) }
                        _ => {
                            warn!("{}",format!("Invalid key {}!", key))
                        }
                    }
                }

                info!("{}: {}", bot_name.clone(), event.clone());

                let mut bot_map = bot_map_mutex.lock().await;

                if event == "get" {
                    let mut text = String::new();

                    for (name, bot) in bot_map.iter() {
                        text += &format!("{} -> {:?}\n", name, bot.event_counts);
                    }
                    
                    let _ = sender.send(Message::Text(text)).await;
                    drop(bot_map);
                    continue;
                }


                let available_bots= bot_map.iter().filter(
                    |&(name, bot)|
                    bot.channel_ids.contains(&channel_id) && 
                    bot.accessible_events.contains(&event.clone()) && 
                    *name != bot_name 
                ).count();

                let bot = bot_map.entry(bot_name.clone()).or_insert(Bot {
                    associated_ip: *peer_addr,
                    accessible_events: HashSet::new(),
                    event_counts: HashMap::new(),
                    channel_ids: HashSet::new()
                });

                bot.channel_ids.insert(channel_id);
                bot.accessible_events.insert(event.clone());

                debug!("{}: {:#?}", bot_name.clone(), bot);

                let mut hasher = FxHasher::default();
                event.clone().hash(&mut hasher);
                message_id.hash(&mut hasher);
                channel_id.hash(&mut hasher);

                let event_id= hasher.finish();
                debug!("Event id: {}", event_id);

                let event_count = bot.event_counts.entry(event.clone()).or_insert(1);
                info!("{}, {}", event_count, available_bots);

                if available_bots >= 1 && *event_count > 3 {
                    info!("Bot has gotten too many events..");
                    let _ = sender.send(Message::Text("handled".to_string())).await;
                    *event_count -= 1;

                    continue;
                }

                let mut handled_events = handled_events_mutex.lock().await;
                if handled_events.contains(&event_id) {
                    info!("Event already handled.. Skipping");
                    let _ = sender.send(Message::Text("handled".to_string())).await;

                    continue;
                }
                
                info!("Letting bot proceed");
                *event_count += 1;
                let _ = sender.send(Message::Text(String::from("unhandled"))).await;

                handled_events.insert(event_id);

                drop(bot_map);
                drop(handled_events);
                println!();
                continue;
            }

            Ok(Message::Close(_)) => {
                let mut bot_map = bot_map_mutex.lock().await;
                
                if let Some(bot_name) = bot_map.iter().find(|&(name, bot)| bot.associated_ip == *peer_addr).map(|(name, bot)| name.clone()) {
                    info!("Dropped bot {}", bot_name);
                    bot_map.remove(&bot_name);
                }
            },
            Ok(_) => (),
            Err(e) => {
                error!("Error decoding message {}", e);
                break;
            }
        }
    }
}
