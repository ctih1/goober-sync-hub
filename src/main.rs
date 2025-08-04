use std::{collections::{HashMap, HashSet}, net::{IpAddr, SocketAddr}, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use futures::{SinkExt, StreamExt};
use log::{error, info, trace, warn};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Clone)]
struct ConnectedBot {
    name: String,
    handled_events: i32,

    accessible_channel_ids: HashSet<u64>,
}

                             // HashMap<MessageId, HashMap<Event, handled>>
type SharedEventMap = Arc<Mutex<HashMap<u64, HashMap<String, bool>>>>;
                                                      // Hashmap<Name, amount of interactions accepted>
type SharedConnectionMap = Arc<Mutex<HashMap<IpAddr, ConnectedBot>>>;

#[tokio::main]
async fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Debug).init();
    
    let address = "0.0.0.0:80";
    let addr: SocketAddr =  address.parse().expect("Failed to parse address");
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");
    let shared_events: SharedEventMap = Arc::new(Mutex::new(HashMap::new()));
    let shared_connection_map: SharedConnectionMap = Arc::new(Mutex::new(HashMap::new()));

    info!("Listening on {}", address);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, Arc::clone(&shared_events), Arc::clone(&shared_connection_map)));
    }
}

async fn handle_connection(stream: TcpStream, shared_events: SharedEventMap, shared_connection_map: SharedConnectionMap) {
    let max_events: i32 = 3;

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
                trace!("Recieved new requests");
                let texts: Vec<&str> = text.split(";").collect();

                let mut event: String = String::new();
                let mut message_id: String = String::new();
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
                        "ref" => { message_id = value.to_string(); }
                        "event" => { event = value.to_string(); }
                        "name" => { bot_name = value.to_string(); }
                        "channel" => { channel_id = value.parse().unwrap_or(1010) }
                        _ => {
                            warn!("{}",format!("Invalid key {}!", key))
                        }
                    }
                }

                if event.is_empty() || message_id.is_empty() || bot_name.is_empty() || channel_id == 1010  {
                    error!("event or message_id not specified!");

                    if let Err(_) = 
                        sender.send(Message::Text("Invalid props passed! Expected ref=...;event=...;name=...;channel=...".to_string())).await
                    {
                        error!("Failed to send invalid message for invalid props");
                        let _ = sender.close().await;
                    }
                    return;
                }


                if &event == "get" {
                    if &message_id == "stats" {
                        info!("Sending stats");
                        let mut text = String::new();
                        for (_, bot) in shared_connection_map.lock().await.clone().into_iter() {
                            
                            text += &format!("{} -> {:#?}\n",bot.name, bot.handled_events);
                        }

                        if let Err(_) = sender.send(Message::Text(text)).await {
                            error!("Failed to send message!");
                            let _ = sender.close().await;
                        }
                    } else {
                        error!("Invalid messageid!");
                    }

                    continue;
                }

                let mut shared_connections = shared_connection_map.lock().await;
                
                let available_connection_amount = shared_connections.iter().filter(
                    |&(_, val)|
                    val.handled_events < max_events && val.accessible_channel_ids.contains(&channel_id)
                ).count();

                let bot_info = shared_connections
                    .entry(peer_addr.ip())
                    .or_insert(ConnectedBot{
                        name: bot_name.clone(), accessible_channel_ids: HashSet::new(), handled_events:0
                });

                info!("Recieved '{}' in {} from {}" , &event, &message_id, &bot_info.name);

                if bot_info.accessible_channel_ids.insert(channel_id) {
                    info!("Added channel {}", &channel_id)
                }

                let mut event_map = shared_events.lock().await;
                let message_events = event_map.entry(message_id.parse().unwrap_or(0)).or_insert(HashMap::new());

                trace!("Available connections: {}", available_connection_amount);

                if bot_info.handled_events >= max_events && available_connection_amount >= 1
                {
                    info!("Too many events for instance {}! Skipping turn", &bot_name);
                    let _ = sender.send(Message::Text("handled".to_string())).await;

                    bot_info.handled_events -= 1;
                    continue;
                }

                let event_handled = message_events.get(&event).unwrap_or(&false);

                if *event_handled {
                    info!("Event already handled");
                    let _ = sender.send(Message::Text("handled".to_string())).await;
                    continue;
                }
                
                info!("Event not handled... Marking as handled");
                message_events.insert(event, true);

                bot_info.handled_events += 1;

                if let Err(_) = sender.send(Message::Text("unhandled".to_string())).await {
                    error!("Failed to send message!");
                    let _ = sender.close().await;
                } 

                drop(event_map);
                drop(shared_connections);
            }

            Ok(Message::Close(_)) => {
                let mut connection_map = shared_connection_map.lock().await;
                connection_map.remove(&peer_addr.ip());
            },

            Ok(_) => (),
            Err(e) => {
                error!("Error decoding message {}", e);
                break;
            }
        }
    }
}