#[cfg(test)]
mod tests {
    use boom_api::conf::{load_config, load_dotenv};
    use std::panic;

    #[test]
    fn test_token_expiration_validation_fails_with_zero() {
        load_dotenv();

        // This should panic because token_expiration is 0
        let result = panic::catch_unwind(|| {
            // Create a temporary config file with token_expiration: 0
            let config_content = r#"
database:
  host: localhost
  port: 27017
  name: test_db
  username: test
  password: test123
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

            // Try to load the config - this should panic due to validation
            load_config(Some(temp_file.path().to_str().unwrap()));
        });

        // The function should have panicked
        assert!(result.is_err());

        // Check that the panic message contains our validation error
        if let Err(panic_info) = result {
            let panic_message = panic_info
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| panic_info.downcast_ref::<&str>().copied())
                .unwrap_or("");

            assert!(panic_message.contains("TOKEN_EXPIRATION must be greater than 0"));
        }
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
  username: test
  password: test123
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
        let config = load_config(Some(temp_file.path().to_str().unwrap()));
        assert_eq!(config.auth.token_expiration, 3600);
    }

    #[test]
    fn test_token_expiration_default_value() {
        load_dotenv();

        // Test that the default value is applied when token_expiration is not specified
        let config_content = r#"
database:
  host: localhost
  port: 27017
  name: test_db
  username: test
  password: test123
api:
  auth:
    secret_key: "test_secret"
    admin_username: admin
    admin_password: test123
    admin_email: admin@test.com
"#;
        let temp_file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        // This should use the default value (7 days = 604800 seconds)
        let config = load_config(Some(temp_file.path().to_str().unwrap()));
        assert_eq!(config.auth.token_expiration, 604800); // 7 days in seconds
    }

    #[test]
    fn test_load_config_from_default_path() {
        load_dotenv();

        // Test loading the actual config.yaml file
        let config = load_config(Some("../config.yaml"));

        // Verify the token_expiration is set to our new default
        assert_eq!(config.auth.token_expiration, 604800); // 7 days in seconds

        // Verify other expected values
        assert!(!config.auth.secret_key.is_empty());
        assert!(!config.auth.admin_password.is_empty());
        assert!(!config.database.password.is_empty());
    }
}
