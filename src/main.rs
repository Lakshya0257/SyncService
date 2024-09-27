use axum::routing::get;
use axum::{
    routing::post,
    Router,
};
use std::net::SocketAddr;
use tokio_postgres::Error;
use tokio::sync::mpsc;
use tokio::task;
// use crate::sheets::update_google_sheet;
use tokio::sync::mpsc::Sender;
use sqlx::postgres::PgListener;
use serde::{Deserialize, Serialize};
use axum::{
    Json,
    response::IntoResponse,
};
use axum::http::StatusCode;
#[tokio::main]
async fn main() -> Result<(), Error> {
    let (tx, mut rx) = mpsc::channel(32);

    let tx_pg = tx.clone();
    task::spawn(async move {
        listen_postgres(tx_pg).await;
    });
    let app = Router::new().route("/webhook", post(webhook_handler)).route("/test", get(|| async  {"Hello World"}));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Listening on {}", addr);

    // let sync_handle = task::spawn(async move {
    //     while let Some(event) = rx.recv().await {
    //         println!("Received event: {}", event);

    //         // Sync data with Google Sheets
    //         update_google_sheet(event).await.unwrap();
    //     }
    // });

    // Start Axum server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    // sync_handle.await.unwrap();

    Ok(())
}



#[derive(Debug, Deserialize, Serialize)]
struct WebhookPayload {
    event: String,
    data: serde_json::Value,
}

// Define a handler for the webhook
async fn webhook_handler(
    payload: String
) -> impl IntoResponse {
    println!("Received webhook: {:?}", payload);
    
    // Here you can handle the payload and perform further actions
    // For example, logging or saving to the database
    
    // Return a success response
    (StatusCode::OK, "Webhook received")
}

#[derive(Debug, Deserialize, Serialize)]
struct NotificationPayload {
    operation: String,
    data: serde_json::Value,
    timestamp: String
}
pub async fn listen_postgres(tx_pg: Sender<String>){
    let url = "postgresql://authentication_owner:wojKQAiL4WF6@ep-silent-credit-a1rfhidk.ap-southeast-1.aws.neon.tech/authentication?sslmode=require";
    let conn = sqlx::postgres::PgPool::connect(url).await;
    match conn {
        Ok(pool)=>{
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
        
                        // Send the notification payload as a JSON string to the `tx_pg` channel
                        if let Err(err) = tx_pg.send(payload_str.to_string()).await {
                            println!("Error sending notification via channel: {}", err);
                        }
                    }
                    Err(err) => {
                        println!("Failed to parse JSON payload: {}", err);
                    }
                }
            }
        },
        Err(e)=>{
            println!("Error connecting postgresql, {}",e.to_string());
        }
    }
}
