use axum::extract::State;
use axum::routing::get;
use axum::{routing::post, Router, Json};
use google_sheets4::chrono;
use serde_json::Value;
use tokio::time;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use sqlx::postgres::PgListener;
use serde::{Deserialize, Serialize};
use redis::{Client as RedisClient, Commands};
use lapin::{Connection, ConnectionProperties, Channel};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct UpdateEvent {
    operation: String,
    datachanged: Option<serde_json::Value>, // Make this optional
    timestamp: String,
    id: String,
    primaryKeyActualName: String,
    fcstatus: Option<serde_json::Value>,
    source: String
}

#[derive(Clone, Debug)]
struct AppState {
    redis: Arc<Mutex<RedisClient>>,
    sheet_queue: mpsc::Sender<UpdateEvent>,
    db_queue: mpsc::Sender<UpdateEvent>,
}

#[shuttle_runtime::main]
async fn axum() -> shuttle_axum::ShuttleAxum {
// #[tokio::main]
// async fn main() {
    // CryptoProvider::install_default().expect("Failed to install CryptoProvider");
    let redis_client = RedisClient::open("redis://default:mqRb9pxI5gQXSOfXOqfB7HRcujF2w1NT@redis-12411.c305.ap-south-1-1.ec2.redns.redis-cloud.com:12411").unwrap();
    let redis = Arc::new(Mutex::new(redis_client));

    // Set up RabbitMQ
    println!("Runing");
    let amqp_addr = "amqp://nywscawh:2QZ9KOc5TJF6U3q1NWcd7MSDsLB39mkC@puffin.rmq2.cloudamqp.com/nywscawh";
    let conn = Connection::connect(amqp_addr, ConnectionProperties::default()).await.unwrap();
    let channel = conn.create_channel().await.unwrap();

    // Create channels for inter-thread communication
    let (sheet_tx, sheet_rx) = mpsc::channel::<UpdateEvent>(100);
    let (db_tx, db_rx) = mpsc::channel::<UpdateEvent>(100);

    // Shared state
    let state = AppState {
        redis: redis.clone(),
        sheet_queue: sheet_tx.clone(),
        db_queue: db_tx.clone(),
    };

    // Set up Axum router
    let app = Router::new()
        // .route("/manual_update", post(handle_manual_update))
        .route("/webhook", post(webhook_handler))
        .route("/test", get(|| async { "Hello World v2" }))
        .with_state(state);

    // Spawn thread for processing sheet updates
    tokio::spawn(process_updates(sheet_rx, channel.clone(), "db_update_queue".to_string(), redis.clone()));

    // Spawn thread for processing db updates
    tokio::spawn(process_updates(db_rx, channel.clone(), "sheet_update_queue".to_string(), redis.clone()));

    // Spawn thread for listening to database events
    tokio::spawn(listen_postgres(sheet_tx.clone()));
    Ok(app.into())

    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    // axum::serve(listener, app).await.unwrap();
}

#[derive(Debug, Deserialize, Serialize)]
struct WebhookPayload {
    event: String,
    data: serde_json::Value,
}

// Define a handler for the webhook
// async fn webhook_handler(payload: String) -> impl IntoResponse {
//     println!("Received webhook: {:?}", payload);
//     (StatusCode::OK, "Webhook received")
// }

async fn webhook_handler(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    println!("Payload: {:?}", payload.get("id").unwrap().to_string());
    println!("Payload: {:?}", payload);

    // Extracting the id from the payload
    let id = payload.get("id").unwrap().to_string();

    // Assuming the operation is part of the payload, adjust as necessary
    let operation = payload["operation"].as_str().unwrap_or("unknown").to_string();

    // Check if the data is present
    let datachanged = payload.get("datachanged").cloned(); // `cloned()` returns an `Option<serde_json::Value>`

    let fcstatus = payload.get("fcstatus").cloned();

    // Create an UpdateEvent
    let event = UpdateEvent {
        id,
        source: "sheet".to_string(),
        datachanged,
        timestamp: chrono::Utc::now().timestamp().to_string(),
        operation, // Set the operation based on the payload
        primaryKeyActualName: "user_id".to_string(), // Change according to your schema
        fcstatus, // Set this appropriately if needed
    };

    // Check for conflicts
    // let conflict = check_conflict(&state.redis, &event).await;

    // if conflict {
    //     return Json(serde_json::json!({ "status": "conflict" }));
    // }

    // Send to db_queue for processing
    if let Err(_) = state.db_queue.send(event).await {
        return Json(serde_json::json!({ "status": "error" }));
    }

    Json(serde_json::json!({ "status": "ok" }))
}
// async fn webhook_handler(
//     State(state): State<AppState>,
//     Json(payload): Json<serde_json::Value>,
// ) -> Json<serde_json::Value> {

//     println!("Payload: {:?}",payload);
//     // Parse the webhook payload and create an UpdateEvent
//     // This parsing logic will depend on the structure of your webhook payload
//     let event = UpdateEvent {
//         id: payload["id"].as_str().unwrap_or("").to_string(),
//         source: "sheet".to_string(),
//         datachanged: payload["data"].to_string(),
//         timestamp: chrono::Utc::now().timestamp().to_string(),
//     };

//     // Check for conflicts
//     let conflict = check_conflict(&state.redis, &event).await;

//     if conflict {
//         return Json(serde_json::json!({ "status": "conflict" }));
//     }

//     // Send to db_queue for processing
//     if let Err(_) = state.db_queue.send(event).await {
//         return Json(serde_json::json!({ "status": "error" }));
//     }

//     Json(serde_json::json!({ "status": "ok" }))
// }


#[derive(Debug, Deserialize, Serialize)]
struct NotificationPayload {
    operation: String,
    datachanged: Option<serde_json::Value>, // Make this optional
    timestamp: String,
    id: String,
    primaryKeyActualName: String,
    fcstatus: Option<serde_json::Value>, // Make this optional
}

async fn listen_postgres(tx_pg: Sender<UpdateEvent>) {
    let url = "postgresql://authentication_owner:wojKQAiL4WF6@ep-silent-credit-a1rfhidk.ap-southeast-1.aws.neon.tech/authentication?sslmode=require";
    let conn = sqlx::postgres::PgPool::connect(url).await;

    match conn {
        Ok(pool) => {
            let mut listener = PgListener::connect_with(&pool).await.unwrap();

            // Subscribe to the "user_changes" channel
            listener.listen("user_changes").await.unwrap();
            println!("Listening for PostgreSQL notifications...");

            // Continuously listen for notifications
            while let Ok(notification) = listener.recv().await {
                let payload_str = notification.payload();
                println!("Received notification: {}", payload_str);

                // Parse the JSON payload
                match serde_json::from_str::<NotificationPayload>(payload_str) {
                    Ok(payload) => {
                        println!("Operation: {}", payload.operation);
                        println!("Data: {:?}", payload.datachanged);
                        println!("Timestamp: {}", payload.timestamp);
                        let event = UpdateEvent {
                            id: payload.id,
                            source: "database".to_string(),
                            datachanged: payload.datachanged,
                            operation: payload.operation,
                            primaryKeyActualName: payload.primaryKeyActualName,
                            fcstatus: payload.fcstatus,
                            timestamp: payload.timestamp.to_string()
                        };

                        // Send the notification payload as a JSON string to the tx_pg channel
                        if let Err(err) = tx_pg.send(event).await {
                            println!("Error sending notification via channel: {}", err);
                        }
                    }
                    Err(err) => {
                        println!("Failed to parse JSON payload: {}", err);
                    }
                }
            }
        }
        Err(e) => {
            println!("Error connecting to PostgreSQL: {}", e.to_string());
        }
    }
}

async fn check_conflict(redis: &Arc<Mutex<RedisClient>>, event: &UpdateEvent) -> bool {
    let mut con = redis.lock().await.get_connection().unwrap();
    let prev_val: Option<HashMap<String,String>> = con.hgetall(format!("cstatus:{}", event.id)).unwrap();
    if event.operation != "delete" {
        // Convert event.datachanged to HashMap<String, String>
        if let Some(datachanged) = &event.fcstatus {
            if let Some(object) = datachanged.as_object() {
                let event_datachanged: HashMap<String, String> = object.iter()
                    .filter_map(|(key, value)| {
                        if let Some(value_str) = value.as_str() {
                            Some((key.clone(), value_str.to_string())) // Create tuple
                        } else {
                            None // Skip non-string values
                        }
                    })
                    .collect();

                // Check if prev_val is the same as event_datachanged
                if let Some(prev) = prev_val {
                    if prev == event_datachanged {
                        return true; // Both HashMaps are the same
                    }
                }
            }
        }
    }
    let last_update: Option<HashMap<String, String>> = con.hgetall(format!("conflictst:{}", event.id)).unwrap();

    if event.operation!="insert".to_string() && event.operation!="delete".to_string()  {
        if let Some(last_update) = last_update {
            // Extract the keys from last_update
            let last_keys: HashSet<String> = last_update.keys().cloned().collect();
    
            if let Some(event_datachanged) = &event.datachanged {
                // Check for conflicts in keys
                let mut conf = false;
                for key in event_datachanged.as_object().unwrap().keys() {
                    if last_keys.contains(key) {
                        conf = true;
                        break;
                    }
                }
                if conf{
                    while conf {
                        let x : Option<HashMap<String,String>> = con.hgetall(format!("conflictst:{}", event.id)).unwrap();
                        if x.is_some() {
                            time::sleep(Duration::from_millis(100)).await;
                        } else{
                            conf = false;
                        }
                    }
                }
            }
        }
    
        let new_data: Vec<(String, String)> = event.datachanged.to_owned().unwrap().as_object().unwrap().iter()
                    .filter_map(|(key, value)| {
                        if let Some(value_str) = value.as_str() {
                            Some((key.clone(), value_str.to_string())) // Create tuple
                        } else {
                            None // Skip non-string values
                        }
                    })
                    .collect();
                    let _: () = con.hset_multiple(format!("conflictst:{}", event.id), &new_data).unwrap();
    }

    if event.operation!="delete".to_string() {
        let new_data: Vec<(String, String)> = event.fcstatus.to_owned().unwrap().as_object().unwrap().iter()
                    .filter_map(|(key, value)| {
                        if let Some(value_str) = value.as_str() {
                            Some((key.clone(), value_str.to_string()))
                        } else {
                            None
                        }
                    })
                    .collect();
                    let _: () = con.hset_multiple(format!("cstatus:{}", event.id), &new_data).unwrap();
                    let _: () = con.expire(format!("cstatus:{}", event.id), 3600).unwrap();
    }
    false
}

async fn process_updates(
    mut rx: mpsc::Receiver<UpdateEvent>, 
    channel: Channel, 
    queue_name: String,
    redis: Arc<Mutex<RedisClient>>
) {
    while let Some(event) = rx.recv().await {
        let conflict = check_conflict(&redis, &event).await;
        if !conflict {
            let payload = serde_json::to_string(&event).unwrap();
            channel.basic_publish(
                "",
                &queue_name,
                lapin::options::BasicPublishOptions::default(),
                payload.as_bytes(),
                lapin::BasicProperties::default(),
            ).await.unwrap();
        } else {
            println!("Conflict detected for event: {:?}", event);
            // Here you can implement your conflict resolution strategy
            // For example, you could send it to a separate queue for manual resolution
            // channel.basic_publish(
            //     "",
            //     "conflict_resolution_queue",
            //     lapin::options::BasicPublishOptions::default(),
            //     serde_json::to_string(&event).unwrap().as_bytes(),
            //     lapin::BasicProperties::default(),
            // ).await.unwrap();
        }
    }
}