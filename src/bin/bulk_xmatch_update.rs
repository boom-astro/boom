use boom::{
    conf::{load_config, load_dotenv, CatalogXmatchConfig},
    utils::{
        db::mongify,
        enums::Survey,
        o11y::logging::build_subscriber,
        spatial::{xmatch, Coordinates},
    },
};
use clap::Parser;
use futures::stream::StreamExt;
use indicatif::ProgressBar;
use mongodb::options::{UpdateOneModel, WriteModel};
use mongodb::{bson::doc, Collection};
use tracing::{error, instrument};

const QUEUE_MULTIPLIER: usize = 2;

#[derive(Parser)]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of processes to use to read the Kafka stream in parallel
    #[arg(long, default_value_t = 1)]
    processes: usize,

    /// Batch size for processing objects
    #[arg(long, default_value_t = 100)]
    batch_size: usize,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
struct AuxCollectionIdOnly {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub coordinates: Coordinates,
}

async fn process_object(
    obj: AuxCollectionIdOnly,
    alert_aux_collection: Collection<AuxCollectionIdOnly>,
    db: mongodb::Database,
    xmatch_config: Vec<CatalogXmatchConfig>,
) -> Option<WriteModel> {
    let (ra, dec) = obj.coordinates.get_radec();

    let xmatches = xmatch(ra, dec, &xmatch_config, &db).await;
    match xmatches {
        Ok(matches) => {
            let update_aux_document = doc! { "$set": {
                "cross_matches": mongify(&matches),
                "updated_at": flare::Time::now().to_jd(),
            }};

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(alert_aux_collection.namespace())
                    .filter(doc! {"_id": obj.object_id})
                    .update(update_aux_document)
                    .build(),
            );
            Some(update)
        }
        Err(e) => {
            error!(
                "Error performing cross-match for object ID {}: {}",
                obj.object_id, e
            );
            None
        }
    }
}

async fn worker(
    survey: Survey,
    config_path: &str,
    receiver: async_channel::Receiver<AuxCollectionIdOnly>,
    batch_size: usize,
) {
    let conf = load_config(Some(config_path)).expect("failed to load configuration");
    let db = conf.build_db().await.expect("failed to build database");
    let client = db.client().clone();
    let alert_aux_collection: Collection<AuxCollectionIdOnly> =
        db.collection(&format!("{}_alerts_aux", survey));
    let xmatch_config = conf
        .crossmatch
        .get(&survey)
        .cloned()
        .expect("no crossmatch configuration for the specified survey");

    let mut objs = Vec::with_capacity(batch_size);
    while let Ok(obj) = receiver.recv().await {
        objs.push(obj);
        if objs.len() >= batch_size {
            let mut updates = Vec::new();
            for obj in objs.drain(..) {
                match process_object(
                    obj,
                    alert_aux_collection.clone(),
                    db.clone(),
                    xmatch_config.clone(),
                )
                .await
                {
                    Some(update) => updates.push(update),
                    None => continue,
                }
            }
            let _ = client.bulk_write(updates).await.unwrap().modified_count;
        }
    }

    if !objs.is_empty() {
        let mut updates = Vec::new();
        for obj in objs.drain(..) {
            match process_object(
                obj,
                alert_aux_collection.clone(),
                db.clone(),
                xmatch_config.clone(),
            )
            .await
            {
                Some(update) => updates.push(update),
                None => continue,
            }
        }
        let _ = client.bulk_write(updates).await.unwrap().modified_count;
    }
}

#[instrument(skip_all, fields(survey = %args.survey))]
async fn run(args: Cli) {
    let conf = load_config(Some(&args.config)).expect("failed to load configuration");
    let db = conf.build_db().await.expect("failed to build database");

    let alert_aux_collection: Collection<AuxCollectionIdOnly> =
        db.collection(&format!("{}_alerts_aux", args.survey));

    let estimated_count = alert_aux_collection
        .estimated_document_count()
        .await
        .expect("failed to get estimated document count");
    let progress_bar = ProgressBar::new(estimated_count as u64)
        .with_message(format!("Updating {} objects cross-matches", args.survey))
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({elapsed_precise}, {eta}, {per_sec})")
                .unwrap(),
        );

    let mut cursor = alert_aux_collection
        .find(doc! {})
        .projection(doc! { "_id": 1, "coordinates.radec_geojson": 1 })
        .await
        .expect("failed to create cursor for auxiliary collection");

    let worker_count = args.processes.max(1);
    let queue_capacity = worker_count * args.batch_size * QUEUE_MULTIPLIER;

    // Create the worker pool
    let (s, r) = async_channel::bounded(queue_capacity);
    let mut workers = Vec::new();
    for _ in 0..worker_count {
        let survey_clone = args.survey.clone();
        let config_path_clone = args.config.clone();
        let receiver_clone = r.clone();
        let worker_handle = tokio::spawn(async move {
            worker(
                survey_clone,
                &config_path_clone,
                receiver_clone,
                args.batch_size,
            )
            .await;
        });
        workers.push(worker_handle);
    }

    let start_time = std::time::Instant::now();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(obj) => {
                if let Err(e) = s.send(obj).await {
                    error!("Failed to send object to workers: {}", e);
                    break;
                }
            }
            Err(e) => {
                error!("Error retrieving document from auxiliary collection: {}", e);
            }
        }
        progress_bar.inc(1);
    }

    drop(s); // close channel, let workers finish

    for worker in workers {
        if let Err(e) = worker.await {
            error!("Worker task failed: {}", e);
        }
    }

    println!(
        "Cross-match update completed in {:.2?} (survey: {}; processes: {}; batch size: {})",
        start_time.elapsed(),
        args.survey,
        args.processes,
        args.batch_size
    );
}
#[tokio::main]
async fn main() {
    // Load environment variables from .env file before anything else
    load_dotenv();

    let args = Cli::parse();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    run(args).await;
}
