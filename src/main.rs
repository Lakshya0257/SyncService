use axum::extract::State;
use axum::routing::{get, post};
use axum::{Router, Json};
use google_sheets4::chrono;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use sqlx::postgres::PgListener;
use serde::{Deserialize, Serialize};
use redis::{Client as RedisClient, Commands};
use lapin::{Connection, ConnectionProperties, Channel};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct UpdateEvent {
    id: String,
    source: String,
    data: String,
    timestamp: String,
}

#[derive(Clone, Debug)]
struct AppState {
    redis: Arc<Mutex<RedisClient>>,
    sheet_queue: mpsc::Sender<UpdateEvent>,
    db_queue: mpsc::Sender<UpdateEvent>,
}

#[shuttle_runtime::main]
pub async fn axum(
    #[shuttle_runtime::Secrets] secrets: shuttle_runtime::SecretStore,
) -> shuttle_axum::ShuttleAxum {
    let redis_client = RedisClient::open("redis://redis-12411.c305.ap-south-1-1.ec2.redns.redis-cloud.com:12411")
        .expect("Failed to connect to Redis");
    let redis = Arc::new(Mutex::new(redis_client));

    // Set up RabbitMQ
    let amqp_addr = "amqps://nywscawh:2QZ9KOc5TJF6U3q1NWcd7MSDsLB39mkC@puffin.rmq2.cloudamqp.com/nywscawh";
    let conn = Connection::connect(amqp_addr, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");
    let channel = conn.create_channel().await.expect("Failed to create channel");

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
        .route("/webhook", post(webhook_handler))
        .route("/test", get(|| async { "Hello World v2" }))
        .with_state(state);

    // Spawn thread for processing sheet updates
    tokio::spawn(process_updates(
        sheet_rx,
        channel.clone(),
        "db_update_queue".to_string(),
        redis.clone(),
    ));

    // Spawn thread for processing db updates
    tokio::spawn(process_updates(
        db_rx,
        channel.clone(),
        "sheet_update_queue".to_string(),
        redis.clone(),
    ));

    // Spawn thread for listening to database events
    tokio::spawn(listen_postgres(sheet_tx.clone()));
    Ok(app.into())
}

#[derive(Debug, Deserialize, Serialize)]
struct WebhookPayload {
    event: String,
    data: serde_json::Value,
}

// Define a handler for the webhook
async fn webhook_handler(
    State(state): State<AppState>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    // Parse the webhook payload and create an UpdateEvent
    let event = UpdateEvent {
        id: payload["id"].as_str().unwrap_or("").to_string(),
        source: "sheet".to_string(),
        data: payload["data"].to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    // Check for conflicts
    let conflict = check_conflict(&state.redis, &event).await;

    if conflict {
        return Json(serde_json::json!({ "status": "conflict" }));
    }

    // Send to db_queue for processing
    if let Err(_) = state.db_queue.send(event).await {
        return Json(serde_json::json!({ "status": "error" }));
    }

    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Debug, Deserialize, Serialize)]
struct NotificationPayload {
    operation: String,
    data: serde_json::Value,
    timestamp: String,
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
                        println!("Data: {}", payload.data);
                        println!("Timestamp: {}", payload.timestamp);
                        let event = UpdateEvent {
                            id: payload.data["id"].to_string(),
                            source: "database".to_string(),
                            data: payload_str.to_string(),
                            timestamp: payload.timestamp.to_string(),
                        };

                        // Send the notification payload as a JSON string to the `tx_pg` channel
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
    let mut con = redis.lock().await.get_connection().expect("Failed to get Redis connection");
    let last_update: Option<String> = con.get(format!("last_update:{}", event.id)).unwrap_or(None);

    if let Some(last_update) = last_update {
        let last_update: UpdateEvent = serde_json::from_str(&last_update).unwrap();
        if last_update.source != event.source && last_update.timestamp > event.timestamp {
            return true;
        }
    }

    let event_json = serde_json::to_string(&event).unwrap();
    let _: () = con.set(format!("last_update:{}", event.id), event_json).unwrap();

    false
}

async fn process_updates(
    mut rx: mpsc::Receiver<UpdateEvent>,
    channel: Channel,
    queue_name: String,
    redis: Arc<Mutex<RedisClient>>,
) {
    while let Some(event) = rx.recv().await {
        let conflict = check_conflict(&redis, &event).await;

        if !conflict {
            let payload = serde_json::to_string(&event).unwrap();
            channel
                .basic_publish(
                    "",
                    &queue_name,
                    lapin::options::BasicPublishOptions::default(),
                    payload.as_bytes(),
                    lapin::BasicProperties::default(),
                )
                .await
                .unwrap();
        } else {
            println!("Conflict detected for event: {:?}", event);
            channel
                .basic_publish(
                    "",
                    "conflict_resolution_queue",
                    lapin::options::BasicPublishOptions::default(),
                    serde_json::to_string(&event).unwrap().as_bytes(),
                    lapin::BasicProperties::default(),
                )
                .await
                .unwrap();
        }
    }
}
