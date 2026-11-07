use boom::{
    alert::{alert_temp_queue_name, recover_temp_queue},
    conf,
    utils::testing::TEST_CONFIG_FILE,
};
use redis::AsyncCommands;

#[tokio::test]
async fn test_recover_temp_queue() {
    let input_queue_name = "TEST_recover_alerts_packets_queue";
    let temp_queue_name = alert_temp_queue_name(input_queue_name);

    let config = conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let mut con = config.build_redis().await.unwrap();
    con.del::<&str, usize>(input_queue_name).await.unwrap();
    con.del::<&str, usize>(temp_queue_name.as_str())
        .await
        .unwrap();

    let orphans: Vec<Vec<u8>> = vec![b"alert_a".to_vec(), b"alert_b".to_vec()];
    con.lpush::<&str, &Vec<Vec<u8>>, usize>(temp_queue_name.as_str(), &orphans)
        .await
        .unwrap();

    let recovered = recover_temp_queue(&mut con, input_queue_name)
        .await
        .unwrap();
    assert_eq!(recovered, 2);
    assert_eq!(
        con.llen::<&str, usize>(temp_queue_name.as_str())
            .await
            .unwrap(),
        0
    );

    let mut requeued: Vec<Vec<u8>> = con
        .lrange::<&str, Vec<Vec<u8>>>(input_queue_name, 0, -1)
        .await
        .unwrap();
    requeued.sort();
    assert_eq!(requeued, vec![b"alert_a".to_vec(), b"alert_b".to_vec()]);

    assert_eq!(
        recover_temp_queue(&mut con, input_queue_name)
            .await
            .unwrap(),
        0
    );

    con.del::<&str, usize>(input_queue_name).await.unwrap();
}
