use boom::kafka::{consume_alerts, produce_from_archive};
use boom::testing_util::{
    alert_worker, drop_alert_collections, empty_processed_alerts_queue, insert_test_filter,
    remove_test_filter,
};
use boom::{conf, filter};
use redis::AsyncCommands;
use std::{env, error::Error, num::NonZero};
use tracing::info;

// puts candids of processed alerts into a redis queue queue_name
pub async fn setup_benchmark(queue_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // remove what's in redis already
    empty_processed_alerts_queue("benchalertpacketqueue", queue_name).await?;
    // drop alert and alert_aux collections in database
    drop_alert_collections("ZTF_alerts", "ZTF_alerts_aux").await?;
    produce_from_archive("20240617", 0, None).await.unwrap();
    consume_alerts("ztf_20240617_programid1", None, true, 0, None)
        .await
        .unwrap();
    info!("processing alerts...");
    alert_worker(
        "ZTF_alerts_packets_queue",
        queue_name,
        "ZTF_alerts",
        "ZTF_alerts_aux",
    )
    .await;
    info!(
        "candids successfully placed into redis queue '{}'",
        queue_name
    );
    Ok(())
}

// run: cargo bench filter_benchmark -- <filter_id> <num_iterations_on_candids>
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let queue_name = "benchmarkqueue";
    setup_benchmark(&queue_name).await?;

    // grab command line arguments
    let args: Vec<String> = env::args().collect();
    let mut filter_id = -1;
    let mut n = 20;
    if args.len() > 3 {
        filter_id = args[2].parse::<i32>().unwrap();
        n = args[3].parse::<i32>().unwrap();
    }

    // connect to mongo and redis
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    // if filter_id is -1, then we are running the benchmark on the test filter
    // as part of the CI/CD pipeline. In which case we need to insert the test filter
    // into the database
    if filter_id == -1 {
        insert_test_filter().await;
    }

    info!("running filter benchmark...");

    let mut runs: Vec<(i32, usize, f64)> = Vec::new();

    let mut test_filter = filter::Filter::build(filter_id, &db).await?;

    // run benchmark n times
    for i in 0..n {
        let start = std::time::Instant::now();
        // retrieve candids from redis queue
        let res: Result<Vec<i64>, redis::RedisError> = con
            .rpop::<&str, Vec<i64>>(&queue_name, NonZero::new(1000))
            .await;

        match res {
            Ok(candids) => {
                if candids.len() == 0 {
                    info!("Queue empty");
                    return Ok(());
                }

                // info!("Found a total of {} candids to process", candids.len());

                let _out_candids = test_filter.run(candids.clone(), &db).await?;

                let total_time = (std::time::Instant::now() - start).as_secs_f64();
                runs.push((i, candids.len(), total_time));

                if i < n - 1 {
                    // push all candids back onto the redis queue
                    con.lpush::<&str, Vec<i64>, isize>(&queue_name, candids.clone())
                        .await?;
                }
            }
            Err(e) => {
                info!("Got error: {:?}", e);
            }
        }
    }
    // info!("=========================\n   FULL OUTPUT\n=========================");
    // for run in runs.clone() {
    //     info!("run {} filtered {} candids in {} seconds", run.0, run.1, run.2);
    // }

    let mut total_alerts = 0;
    let mut total_time = 0.0;
    let mut min_time: (i32, f64) = (-1, 99999.0);
    let mut max_time: (i32, f64) = (-1, 0.0);
    for i in runs.clone() {
        total_alerts += i.1;
        total_time += i.2;
        if i.2 < min_time.1 {
            min_time = (i.0, i.2);
        }
        if i.2 > max_time.1 {
            max_time = (i.0, i.2);
        }
    }
    info!("=========================\n   SUMMARY\n");
    let total_alerts = total_alerts as f64;
    let average = total_alerts / total_time;
    info!("   average speed: {} alerts filtered / sec", average);
    info!(
        "   fastest run: {} @ {}\n   slowest run: {} @ {}",
        min_time.0, min_time.1, max_time.0, max_time.1
    );
    info!("=========================");

    // if filter_id is -1, then we are running the benchmark on the test filter
    // we remove it from the database
    if filter_id == -1 {
        remove_test_filter().await;
    }

    Ok(())
}
