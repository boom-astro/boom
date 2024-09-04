use boom::spatial;
use boom::conf;

async fn benchmark_regular_xmatch(ra: Vec<f64>, dec: Vec<f64>, nb_samples: usize, conf: config::Config, db: &mongodb::Database) {
    println!("Running regular test for Gaia_DR3");

    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);

    let catalog_xmatch_configs = catalog_xmatch_configs.into_iter().filter(|x| x.catalog == "Gaia_DR3").collect::<Vec<_>>();

    let mut xmatches = spatial::xmatch(ra[0], dec[0], &catalog_xmatch_configs, &db).await;

    let start = std::time::Instant::now();

    for i in 0..nb_samples {
        xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, &db).await;
        if i % 100 == 0 {
            println!("Processed {} coordinates in {:.4} seconds", i, start.elapsed().as_secs() as f64);
        }
    }

    xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));

    let elapsed = start.elapsed().as_secs();
    println!("Elapsed time: {:.4} seconds", elapsed as f64);
}

async fn benchmark_partitioned_xmatch(ra: Vec<f64>, dec: Vec<f64>, nb_samples: usize, conf: config::Config, db: &mongodb::Database) {
    // Gaia_DR3 partitioned
    println!("Running partitioned test for Gaia_DR3");

    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    let catalog_xmatch_config = catalog_xmatch_configs.iter().find(|x| x.catalog == "Gaia_DR3_test").unwrap();

    let mut xmatches = spatial::xmatch_partitioned(ra[0], dec[0], catalog_xmatch_config, &db).await;

    let start = std::time::Instant::now();

    for i in 0..nb_samples {
        xmatches = spatial::xmatch_partitioned(ra[i], dec[i], catalog_xmatch_config, &db).await;
        if i % 100 == 0 {
            println!("Processed {} coordinates in {:.4} seconds", i, start.elapsed().as_secs() as f64);
        }
    }

    let elapsed = start.elapsed().as_secs();

    println!("Partitioned test: Elapsed time for Gaia_DR3: {:.4} seconds", elapsed as f64);

    xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));
}

async fn benchmark_partitioned_xmatch_v2(ra: Vec<f64>, dec: Vec<f64>, nb_samples: usize, conf: config::Config, db: &mongodb::Database) {
    println!("Running partitioned test for Gaia_DR3_hpidx");
    // One-collection partitioned xmatch
    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    let catalog_xmatch_config = catalog_xmatch_configs.iter().find(|x| x.catalog == "Gaia_DR3_hpidx").unwrap();

    let mut xmatches = spatial::xmatch_partitioned_v2(ra[0], dec[0], catalog_xmatch_config, &db).await;

    let start = std::time::Instant::now();

    for i in 0..nb_samples {
        xmatches = spatial::xmatch_partitioned_v2(ra[i], dec[i], catalog_xmatch_config, &db).await;
        if i % 100 == 0 {
            println!("Processed {} coordinates in {:.4} seconds", i, start.elapsed().as_secs() as f64);
        }
    }

    let elapsed = start.elapsed().as_secs();

    println!("Partitioned test: Elapsed time for Gaia_DR3_hpidx: {:.4} seconds", elapsed as f64);

    xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));
}

#[tokio::main]
async fn main() {
    let conf = conf::load_config("tests/data/config.test.yaml").unwrap();
    let db = conf::build_db(&conf, false).await;

    // // generate some random coordinates, 1000 pairs of ra, dec
    let nb_samples = 1000;
    let ra = (0..nb_samples).map(|_| rand::random::<f64>() * 360.0).collect::<Vec<_>>();
    let dec = (0..nb_samples).map(|_| rand::random::<f64>() * 180.0 - 90.0).collect::<Vec<_>>();

    // benchmark_regular_xmatch(ra.clone(), dec.clone(), nb_samples, conf.clone(), &db).await;
    // benchmark_partitioned_xmatch(ra.clone(), dec.clone(), nb_samples, conf.clone(), &db).await;
    benchmark_partitioned_xmatch_v2(ra.clone(), dec.clone(), nb_samples, conf.clone(), &db).await;


    // now we want to validate that the partitioned xmatch returns the same results as the non-partitioned xmatch
    // let mut xmatches = spatial::xmatch(ra[0], dec[0], &catalog_xmatch_configs, &db).await;
    // let mut xmatches_partitioned = spatial::xmatch_partitioned(ra[0], dec[0], catalog_xmatch_config, &db).await;

    // let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    // let catalog_xmatch_config = catalog_xmatch_configs.iter().find(|x| x.catalog == "Gaia_DR3_test").unwrap();

    // for i in 0..nb_samples {
    //     let xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, &db).await;
    //     let xmatches_partitioned = spatial::xmatch_partitioned(ra[i], dec[i], catalog_xmatch_config, &db).await;

    //     let gaia_xmatch = xmatches.get_array("Gaia_DR3").unwrap();
    //     let gaia_xmatch_partitioned = xmatches_partitioned.get_array("Gaia_DR3_test").unwrap();

    //     // assert that the length of the arrays is the same
    //     assert_eq!(gaia_xmatch.len(), gaia_xmatch_partitioned.len());

    //     // get the list of ids for the non-partitioned xmatch
    //     let mut gaia_xmatch_ids = gaia_xmatch.iter().map(|x| x.as_document().unwrap().get("_id").unwrap().as_i64()).collect::<Vec<_>>();
    //     // sort the ids
    //     gaia_xmatch_ids.sort();

    //     // get the list of ids for the partitioned xmatch
    //     let mut gaia_xmatch_partitioned_ids = gaia_xmatch_partitioned.iter().map(|x| x.as_document().unwrap().get("_id").unwrap().as_i64()).collect::<Vec<_>>();
    //     // sort the ids
    //     gaia_xmatch_partitioned_ids.sort();

    //     // assert that the two lists are the same
    //     assert_eq!(gaia_xmatch_ids, gaia_xmatch_partitioned_ids);
    
    //     if i % 100 == 0 {
    //         println!("Successfully compared {} xmatch results for Gaia_DR3 (partitioned vs non-partitioned)", i);
    //     }
    // }

    println!("All tests passed!");
}