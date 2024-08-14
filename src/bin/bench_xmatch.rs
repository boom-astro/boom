use boom::spatial;
use boom::conf;

#[tokio::main]
async fn main() {
    let conf = conf::load_config("tests/data/config.test.yaml").unwrap();
    let db = conf::build_db(&conf).await;

    // let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);

    // // keep everything but CLU and NED
    // let catalog_xmatch_configs = catalog_xmatch_configs.into_iter().filter(|x| x.catalog != "CLU" && x.catalog != "NED").collect::<Vec<_>>();

    // // generate some random coordinates, 1000 pairs of ra, dec
    let nb_samples = 10000;
    let ra = (0..nb_samples).map(|_| rand::random::<f64>() * 360.0).collect::<Vec<_>>();
    let dec = (0..nb_samples).map(|_| rand::random::<f64>() * 180.0 - 90.0).collect::<Vec<_>>();

    // let mut xmatches = spatial::xmatch(ra[0], dec[0], &catalog_xmatch_configs, &db).await;

    // // start a timer
    // let start = std::time::Instant::now();

    // // run the xmatch 1000 times
    // for i in 0..10 {
    //     xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, &db).await;
    //     // let _ps1_xmatch = &xmatches.get_array("PS1_DR1").unwrap();
    // }

    // // stop the timer
    // let elapsed = start.elapsed().as_secs();
    // println!("Elapsed time: {:.4} seconds", elapsed as f64);

    // print the number of xmatches per catalog
    // xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));

    // // now same thing but with only CLU and NED
    // let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);

    // // keep only CLU and NED
    // let catalog_xmatch_configs = catalog_xmatch_configs.into_iter().filter(|x| x.catalog == "CLU" || x.catalog == "NED").collect::<Vec<_>>();

    // let start = std::time::Instant::now();

    // for i in 0..10 {
    //     xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, &db).await;
    //     // let _ps1_xmatch = &xmatches.get_array("PS1_DR1").unwrap();
    // }

    // let elapsed = start.elapsed().as_secs();
    // println!("Elapsed time: {:.4} seconds", elapsed as f64);

    // xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));


    // now, do that benchmark one catalog at a time, to see what catalog is taking the longest
    // async fn xmatch_catalog(ra: Vec<f64>, dec: Vec<f64>, db: &mongodb::Database, catalog: &str, conf: &config::Config) {
    //     let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    //     let catalog_xmatch_configs = catalog_xmatch_configs.into_iter().filter(|x| x.catalog == catalog).collect::<Vec<_>>();
    //     let mut xmatches = spatial::xmatch(ra[0], dec[0], &catalog_xmatch_configs, db).await;

    //     let start = std::time::Instant::now();
    //     for i in 0..1000 {
    //         xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, db).await;
    //     }
    //     let elapsed = start.elapsed().as_secs();

    //     println!("Elapsed time for {}: {:.4} seconds", catalog, elapsed as f64);
    // }

    // xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));

    // // extract the catalog names from the catalog_xmatch_configs
    // let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    // let catalog_names = catalog_xmatch_configs.iter().map(|x| x.catalog.clone()).collect::<Vec<_>>();

    // for catalog in catalog_names {
    //     xmatch_catalog(ra.clone(), dec.clone(), &db, &catalog, &conf).await;
    // }

    // PARTITIONED XMATCH BENCHMARK

    // // nGaia_DR3 only
    //  let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);

    //  let catalog_xmatch_configs = catalog_xmatch_configs.into_iter().filter(|x| x.catalog == "Gaia_DR3").collect::<Vec<_>>();
 
    //  let mut xmatches = spatial::xmatch(ra[0], dec[0], &catalog_xmatch_configs, &db).await;
 
    //  let start = std::time::Instant::now();
 
    //  for i in 0..nb_samples {
    //     xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, &db).await;
    //     if i % 100 == 0 {
    //         println!("Processed {} coordinates in {:.4} seconds", i, start.elapsed().as_secs() as f64);
    //     }
    //  }
 
    //  xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));
 
    //  let elapsed = start.elapsed().as_secs();
    //  println!("Elapsed time: {:.4} seconds", elapsed as f64);


    // // Gaia_DR3 partitioned
    // println!("Running partitioned test for Gaia_DR3");

    // let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    // let catalog_xmatch_config = catalog_xmatch_configs.iter().find(|x| x.catalog == "Gaia_DR3_test").unwrap();

    // let mut xmatches = spatial::xmatch_partitioned(ra[0], dec[0], catalog_xmatch_config, &db).await;

    // let start = std::time::Instant::now();

    // for i in 0..nb_samples {
    //     xmatches = spatial::xmatch_partitioned(ra[i], dec[i], catalog_xmatch_config, &db).await;
    //     if i % 100 == 0 {
    //         println!("Processed {} coordinates in {:.4} seconds", i, start.elapsed().as_secs() as f64);
    //     }
    // }

    // let elapsed = start.elapsed().as_secs();

    // println!("Partitioned test: Elapsed time for Gaia_DR3: {:.4} seconds", elapsed as f64);

    // xmatches.iter().for_each(|(k, v)| println!("{}: {}", k, v));

    // now we want to validate that the partitioned xmatch returns the same results as the non-partitioned xmatch
    // let mut xmatches = spatial::xmatch(ra[0], dec[0], &catalog_xmatch_configs, &db).await;
    // let mut xmatches_partitioned = spatial::xmatch_partitioned(ra[0], dec[0], catalog_xmatch_config, &db).await;

    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    let catalog_xmatch_config = catalog_xmatch_configs.iter().find(|x| x.catalog == "Gaia_DR3_test").unwrap();

    for i in 0..nb_samples {
        let xmatches = spatial::xmatch(ra[i], dec[i], &catalog_xmatch_configs, &db).await;
        let xmatches_partitioned = spatial::xmatch_partitioned(ra[i], dec[i], catalog_xmatch_config, &db).await;

        let gaia_xmatch = xmatches.get_array("Gaia_DR3").unwrap();
        let gaia_xmatch_partitioned = xmatches_partitioned.get_array("Gaia_DR3_test").unwrap();

        // assert that the length of the arrays is the same
        assert_eq!(gaia_xmatch.len(), gaia_xmatch_partitioned.len());

        // get the list of ids for the non-partitioned xmatch
        let mut gaia_xmatch_ids = gaia_xmatch.iter().map(|x| x.as_document().unwrap().get("_id").unwrap().as_i64()).collect::<Vec<_>>();
        // sort the ids
        gaia_xmatch_ids.sort();

        // get the list of ids for the partitioned xmatch
        let mut gaia_xmatch_partitioned_ids = gaia_xmatch_partitioned.iter().map(|x| x.as_document().unwrap().get("_id").unwrap().as_i64()).collect::<Vec<_>>();
        // sort the ids
        gaia_xmatch_partitioned_ids.sort();

        // assert that the two lists are the same
        assert_eq!(gaia_xmatch_ids, gaia_xmatch_partitioned_ids);
    
        if i % 100 == 0 {
            println!("Successfully compared {} xmatch results for Gaia_DR3 (partitioned vs non-partitioned)", i);
        }
    }

    println!("All tests passed!");
}