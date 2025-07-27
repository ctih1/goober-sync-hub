use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};

                             // HashMap<MessageId, HashMap<Event, handled>>
type SharedEventMap = Arc<Mutex<HashMap<String, HashMap<String, bool>>>>;
                            // Hashmap<Name, amount of interactions accepted>
type SharedConnectionMap = Arc<Mutex<HashMap<SocketAddr, HashMap<String, i32>>>>;

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
    let max_events = 3;
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
                info!("Recieved event {}",text);
                let texts: Vec<&str> = text.split(";").collect();

                let mut event: String = String::new();
                let mut message_id: String = String::new();
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
                        _ => {
                            warn!("{}",format!("Invalid key {}!", key))
                        }
                    }
                }

                if event.is_empty() || message_id.is_empty() || bot_name.is_empty() {
                    error!("event or message_id not specified!");
                    if let Err(_) = sender.send(Message::Text("Invalid props passed! Expected ref=...;event=...;name=...".to_string())).await {
                        error!("Failed to send invalid message for invalid props");
                        let _ = sender.close().await;
                    }
                    return;
                }
                

                let mut map = shared_events.lock().await;
                let message_events = map.entry(message_id).or_insert(HashMap::new());
                let event_handled = &message_events.get(&event);


                let mut shared_connections = shared_connection_map.lock().await;
                let amount_of_available_conns = shared_connections.iter().filter(
                    |&(_, val)|
                    val.get(&bot_name).unwrap_or(&0) < &max_events
                ).count();
                let handled_events = shared_connections.entry(*peer_addr).or_insert(HashMap::new()).entry(bot_name.clone()).or_insert(0);    

                if *handled_events >= max_events && amount_of_available_conns <= 1 {
                    info!("Too many events for instance {}! Skipping turn", &bot_name);
                    let _ = sender.send(Message::Text("handled".to_string())).await;

                    *handled_events -= 1;
                } else {
                    if let Some(_) = event_handled {
                        info!("Event already handled");
                        let _ = sender.send(Message::Text("handled".to_string())).await;
                    } else {
                        info!("Event not handled... Marking as handled");
                        message_events.insert(event, true);
    
                        *handled_events += 1;
        
                        if let Err(_) = sender.send(Message::Text("unhandled".to_string())).await {
                            error!("Failed to send message!");
                            let _ = sender.close().await;
                        }
                    }
                }

                for (key, value) in shared_connections.clone().into_iter() {
                    println!("{}({}) -> {:#?}", value.keys().last().unwrap_or(&String::from("")), key, value.values().last().unwrap_or(&0));
                }     

                drop(map);
                drop(shared_connections);
                

            }
            Ok(Message::Close(_)) => {
                let mut connection_map = shared_connection_map.lock().await;
                connection_map.remove(peer_addr);
            },
            Ok(_) => (),
            Err(e) => {
                error!("Error decoding message {}", e);
                break;
            }
        }
    }
}