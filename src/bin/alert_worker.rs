use redis::AsyncCommands;
use std::env;

use boom::conf;
use boom::alert;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    // user can pass the path to a config file, but it is optional.
    // if not provided, we use the default config.default.yaml
    let config_file = if args.len() > 1 {
        conf::load_config(&args[1]).unwrap()
    } else {
        println!("No config file provided, using default config.default.yaml");
        conf::load_config("./config.default.yaml").unwrap()
    };

    let xmatch_configs = conf::build_xmatch_configs(&config_file);
    let db: mongodb::Database = conf::build_db(&config_file, true).await;

    if let Err(e) = db.list_collection_names().await {
        println!("Error connecting to the database: {}", e);
        return;
    }

    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis.get_multiplexed_async_connection().await.unwrap();

    let mut count = 0;
    let start = std::time::Instant::now();
    loop {
        let result: Option<Vec<Vec<u8>>> = con.rpoplpush("alertpacketqueue", "alertpacketqueuetemp").await.unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(value[0].clone(), &xmatch_configs, &db).await;
                match candid {
                    Ok(Some(candid)) => {
                        println!("Processed alert with candid: {}, queueing for classification", candid);
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>("alertclassifierqueue", candid).await.unwrap();
                        con.lrem::<&str, Vec<u8>, isize>("alertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                    }
                    Ok(None) => {
                        println!("Alert already exists");
                        // remove the alert from the queue
                        con.lrem::<&str, Vec<u8>, isize>("alertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                    }
                    Err(e) => {
                        println!("Error processing alert: {}, requeueing", e);
                        // put it back in the alertpacketqueue, to the left (pop from the right, push to the left)
                        con.lrem::<&str, Vec<u8>, isize>("alertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                        con.lpush::<&str, Vec<u8>, isize>("alertpacketqueue", value[0].clone()).await.unwrap();
                    }
                }

                if count > 1 && count % 100 == 0 {
                    let elapsed = start.elapsed().as_secs();
                    println!("\nProcessed {} alerts in {} seconds, avg: {:.4} alerts/s\n", count, elapsed, elapsed as f64 / count as f64);
                }
                count += 1;
            }
            None => {
                println!("Queue is empty");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

}