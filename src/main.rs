use std::{clone, collections::{HashMap, HashSet, VecDeque}, hash::{Hash, Hasher}, net::{IpAddr, SocketAddr}, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use futures::{SinkExt, StreamExt};
use log::{error, info, debug, warn};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use fxhash::FxHasher;
use async_std::task;

type EventKey = String;
type EventHash = u64;

#[derive(Debug)]
struct Waiting {
    bot_name: String,
    start_timestamp: u64
}

#[derive(Debug, Clone)]
struct Bot {
    associated_ip: SocketAddr,
    accessible_events: HashSet<EventKey>,
    event_counts: HashMap<EventKey, i32>,
    channel_ids: HashSet<u64>
}


type BotMap = Arc<Mutex<HashMap<String, Bot>>>;
type HandledEvents = Arc<Mutex<HashSet<EventHash>>>;
type WaitingMap = Arc<Mutex<HashMap<EventHash, Waiting>>>;

#[tokio::main]
async fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Debug).init();
    
    let address = "0.0.0.0:3960";
    let addr: SocketAddr =  address.parse().expect("Failed to parse address");
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");

    let bot_map: BotMap = Arc::new(Mutex::new(HashMap::new()));
    let handled_events: HandledEvents = Arc::new(Mutex::new(HashSet::new()));
    let waiting_map: WaitingMap = Arc::new(Mutex::new(HashMap::new()));

    info!("Listening on {}", address);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_connection(
                stream,
                Arc::clone(&bot_map),
                Arc::clone(&handled_events),
                Arc::clone(&waiting_map)
        ));
    }
}

async fn handle_connection(
    stream: TcpStream,
    bot_map_mutex: BotMap,
    handled_events_mutex: HandledEvents,
    waiting_map_mutex: WaitingMap
) {
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
                println!();

                let texts: Vec<&str> = text.split(";").collect();

                let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

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

                let mut hasher = FxHasher::default();
                event.clone().hash(&mut hasher);
                channel_id.hash(&mut hasher);                
                message_id.hash(&mut hasher);

                let event_id = hasher.finish();
                debug!("Event id: {}", event_id);

                let available_bots: HashMap<String, Bot> = bot_map.iter().filter(
                    |&(name, bot)|
                    bot.channel_ids.contains(&channel_id) && 
                    bot.accessible_events.contains(&event.clone()) && 
                    *name != bot_name
                ).map(|(name, bot)| (name.clone(), bot.clone())).collect();

                let bot = bot_map.entry(bot_name.clone()).or_insert(Bot {
                    associated_ip: *peer_addr,
                    accessible_events: HashSet::new(),
                    event_counts: HashMap::new(),
                    channel_ids: HashSet::new(),
                });

                bot.channel_ids.insert(channel_id);
                bot.accessible_events.insert(event.clone());

                debug!("{}: {:#?}", bot_name.clone(), bot);

                let waiting_map = waiting_map_mutex.lock().await;
                let waiting_entry = waiting_map.get(&event_id.clone());

                if let Some(t_bot) = waiting_entry && t_bot.bot_name != bot_name {
                    info!("{:#?}", waiting_map);
                    info!("Other bot already in queue.. skiping");
                    let _ = sender.send(Message::Text("handled".to_string())).await;
                    
                    continue;
                }

                drop(waiting_map);

                let mut handled_events = handled_events_mutex.lock().await;
                if handled_events.contains(&event_id) {
                    info!("Event already handled.. Skipping");
                    let _ = sender.send(Message::Text("handled".to_string())).await;


                    continue;
                }

                drop(handled_events);
                
                if !bot.event_counts.contains_key(&event.clone()) {
                    bot.event_counts.insert(event.clone(), 0);
                }

                let mut bot_event_counts = bot.event_counts.iter().map(|(name, amount)| (name.clone(), *amount)).collect::<HashMap<String, i32>>();
                let mut event_count = *bot_event_counts.entry(event.clone()).or_insert(0);
            
                let target_bot = available_bots.iter().find(|&(name, t_bot)| name == "squircle.macos");

                if let Some((name, bot)) = target_bot {
                    info!("Giving turn to {}", name);
                    let _ = sender.send(Message::Text("handled".to_string())).await;

                    let mut waiting_map = waiting_map_mutex.lock().await;
                    waiting_map.insert(event_id, Waiting { bot_name: name.clone(), start_timestamp: epoch });
                    
                    drop(waiting_map);
                    drop(bot_map);


                    for _ in 0..15 { // wait for 2.5 secs to see if the bot has accepted the request
                        task::sleep(Duration::from_millis(100)).await
                    }

                    let mut waiting_map = waiting_map_mutex.lock().await;
                    bot_map = bot_map_mutex.lock().await;

                    if waiting_map.contains_key(&event_id) {
                        info!("Stupid bot did not respond, giving this instead and resetting other");
                    } else {
                        debug!("Other bot succesfully got request");
                        continue
                    }
                }
                
                info!("Letting bot proceed");
                event_count += 1;
                bot_map.get_mut(&bot_name.clone()).unwrap().event_counts.insert(event.clone(), event_count);

                let _ = sender.send(Message::Text(String::from("unhandled"))).await;

                let mut waiting_map = waiting_map_mutex.lock().await;
                waiting_map.remove(&event_id);
                drop(waiting_map);

                let mut handled_events = handled_events_mutex.lock().await;
                handled_events.insert(event_id);

                drop(handled_events);
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
