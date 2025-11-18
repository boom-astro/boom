use boom::conf::{self, build_db, load_config, load_dotenv, AppConfig};
use boom::utils::testing::TEST_CONFIG_FILE;

#[test]
fn test_load_raw_config() {
    let config = conf::load_raw_config(TEST_CONFIG_FILE);
    assert!(config.is_ok());

    let config = config.unwrap();

    let hello = config.get_string("hello");
    assert!(hello.is_ok());

    let hello = hello.unwrap();
    assert_eq!(hello, "world");
}

#[tokio::test]
async fn test_build_db() {
    let config = AppConfig::from_test_config().unwrap();
    let db = build_db(&config).await.unwrap();
    // try a simple query to just validate that the connection works
    let _collections = db.list_collection_names().await.unwrap();
}

#[test]
fn test_load_xmatch_configs() {
    let config = AppConfig::from_test_config().unwrap();

    let crossmatch_config_ztf = config
        .crossmatch
        .get(&boom::utils::enums::Survey::Ztf)
        .cloned()
        .unwrap_or_default();
    assert!(crossmatch_config_ztf.len() > 0);
    for crossmatch in crossmatch_config_ztf.iter() {
        assert!(crossmatch.catalog.len() > 0);
        assert!(crossmatch.radius > 0.0);
        assert!(crossmatch.projection.len() > 0);
    }

    let first = &crossmatch_config_ztf[0];
    assert_eq!(first.catalog, "PS1_DR1");
    assert_eq!(first.radius, 2.0 * std::f64::consts::PI / 180.0 / 3600.0);
    assert_eq!(first.use_distance, false);
    assert_eq!(first.distance_key, None);
    assert_eq!(first.distance_max, None);
    assert_eq!(first.distance_max_near, None);
    let projection = &first.projection;
    // test reading a few of the expected fields
    assert_eq!(projection.get("_id").unwrap().as_i64().unwrap(), 1);
    assert_eq!(projection.get("gMeanPSFMag").unwrap().as_i64().unwrap(), 1);
    assert_eq!(
        projection.get("gMeanPSFMagErr").unwrap().as_i64().unwrap(),
        1
    );
}

#[test]
#[should_panic(expected = "Token expiration must be greater than 0")]
fn test_token_expiration_validation_fails_with_zero() {
    load_dotenv();

    // Create a temporary config file with token_expiration: 0
    let config_content = r#"
database:
host: localhost
port: 27017
name: test_db
max_pool_size: 200
replica_set: null
username: test
password: test123
srv: false
api:
auth:
secret_key: "test_secret"
token_expiration: 0
admin_username: admin
admin_password: test123
admin_email: admin@test.com
"#;
    let temp_file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
    std::fs::write(temp_file.path(), config_content).unwrap();

    // Trigger the panic (the #[should_panic] attribute will assert on message substring)
    load_config(Some(temp_file.path().to_str().unwrap())).unwrap();
}

#[test]
fn test_token_expiration_validation_passes_with_valid_value() {
    load_dotenv();

    // This should work fine with a valid token_expiration
    let config_content = r#"
database:
host: localhost
port: 27017
name: test_db
max_pool_size: 200
replica_set: null
username: test
password: test123
srv: false
api:
auth:
secret_key: "test_secret"
token_expiration: 3600
admin_username: admin
admin_password: test123
admin_email: admin@test.com
"#;
    let temp_file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
    std::fs::write(temp_file.path(), config_content).unwrap();

    // This should not panic
    let config = load_config(Some(temp_file.path().to_str().unwrap())).unwrap();
    assert_eq!(config.api.auth.token_expiration, 3600);
}

#[test]
fn test_token_expiration_with_standard_value() {
    load_dotenv();

    // Test that the standard token_expiration value (7 days) works correctly
    let config_content = r#"
database:
host: localhost
port: 27017
name: test_db
max_pool_size: 200
replica_set: null
username: test
password: test123
srv: false
api:
auth:
secret_key: "test_secret"
token_expiration: 604800
admin_username: admin
admin_password: test123
admin_email: admin@test.com
"#;
    let temp_file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
    std::fs::write(temp_file.path(), config_content).unwrap();

    // This should load successfully with the standard 7-day expiration
    let config = load_config(Some(temp_file.path().to_str().unwrap())).unwrap();
    assert_eq!(config.api.auth.token_expiration, 604800); // 7 days in seconds
}

#[test]
fn test_load_config_from_default_path() {
    load_dotenv();

    // Test loading the test config file which has actual values for secrets
    let config = AppConfig::from_test_config().unwrap();

    // Verify the token_expiration is set to our new default
    assert_eq!(config.api.auth.token_expiration, 604800); // 7 days in seconds

    // Verify other expected values
    assert!(!config.api.auth.secret_key.is_empty());
    assert!(!config.api.auth.admin_password.is_empty());
    assert!(!config.database.password.is_empty());
}
