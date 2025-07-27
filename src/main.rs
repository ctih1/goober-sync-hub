use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};

                            // HashMap<MessageId, HashMap<Event, handled>>
type SharedEventMap = Arc<Mutex<HashMap<String, HashMap<String, bool>>>>;


#[tokio::main]
async fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Debug).init();
    
    let address = "0.0.0.0:80";
    let addr: SocketAddr =  address.parse().expect("Failed to parse address");
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");
    let shared_events: SharedEventMap = Arc::new(Mutex::new(HashMap::new()));

    info!("Listening on {}", address);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, Arc::clone(&shared_events)));
    }
}

async fn handle_connection(stream: TcpStream, shared_events: SharedEventMap) {
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
                        _ => {
                            warn!("{}",format!("Invalid key {}!", key))
                        }
                    }
                }

                if event.is_empty() || message_id.is_empty() {
                    error!("event or message_id not specified!");
                    if let Err(_) = sender.send(Message::Text("Invalid props passed! Expected ref=...;event=...".to_string())).await {
                        error!("Failed to send invalid message for invalid props");
                        let _ = sender.close().await;
                    }
                    return;
                }

                let mut map = shared_events.lock().await;
                let message_events = map.entry(message_id).or_insert(HashMap::new());
                let event_handled = &message_events.get(&event);

                
                if let Some(_) = event_handled {
                    info!("Event already handled");
                    let _ = sender.send(Message::Text("handled".to_string())).await;
                } else {
                    info!("Event not handled... Marking as handled");
                    message_events.insert(event, true);
                    drop(map);
    
                    if let Err(_) = sender.send(Message::Text("unhandled".to_string())).await {
                        error!("Failed to send message!");
                        let _ = sender.close().await;
                    }
                }               
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => (),
            Err(e) => {
                error!("Error decoding message {}", e);
                break;
            }
        }
    }
}