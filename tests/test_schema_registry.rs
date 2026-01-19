use boom::alert::{SchemaRegistry, LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL};

/// Test that the GitHub fallback works when schema registry is unreachable
#[tokio::test]
async fn test_schema_registry_github_fallback() {
    // Create a SchemaRegistry pointing to a non-existent/unreachable URL
    // This ensures the schema registry lookup will fail immediately
    let mut registry = SchemaRegistry::new(
        "http://localhost:9999",
        Some(LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL.to_string()),
    );

    // Try to get a schema that doesn't exist in the unreachable registry
    // This should fail on the registry side, but then fallback to GitHub
    let result = registry.get_schema("alert-packet", 1000).await;

    // The result should be Ok with a valid schema, fetched from GitHub
    assert!(
        result.is_ok(),
        "Should successfully fetch schema from GitHub fallback"
    );

    let schema = result.unwrap();
    // The schema should have a name field indicating it's from the alert packet
    let schema_str = schema.canonical_form();
    assert!(
        schema_str.contains("lsst"),
        "Schema should contain LSST references"
    );
    assert!(
        schema_str.contains("alert"),
        "Schema should be an alert schema"
    );
}

/// Test that the GitHub fallback resolves nested schema references correctly
#[tokio::test]
async fn test_schema_registry_github_fallback_nested_refs() {
    let mut registry = SchemaRegistry::new(
        "http://localhost:9999",
        Some(LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL.to_string()),
    );

    // Get the schema which should resolve all nested references
    let result = registry.get_schema("alert-packet", 1000).await;
    assert!(
        result.is_ok(),
        "Should successfully fetch schema from GitHub"
    );

    let schema = result.unwrap();
    let schema_str = schema.canonical_form();

    // The resolved schema should contain references to nested types
    // Common nested types in LSST alerts include:
    // - diaSource
    // - diaObject
    // - ssSource
    // - diaForcedSource
    // - mpc_orbits
    assert!(
        schema_str.contains("diaSource"),
        "Schema should contain diaSource type"
    );
    assert!(
        schema_str.contains("diaObject"),
        "Schema should contain diaObject type"
    );
    assert!(
        schema_str.contains("ssSource"),
        "Schema should contain ssSource type"
    );
    assert!(
        schema_str.contains("diaForcedSource"),
        "Schema should contain diaForcedSource type"
    );
    assert!(
        schema_str.contains("mpc_orbits"),
        "Schema should contain mpc_orbits type"
    );
}

/// Test that the schema is cached after first fetch
#[tokio::test]
async fn test_schema_registry_caching() {
    let mut registry = SchemaRegistry::new(
        "http://localhost:9999",
        Some(LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL.to_string()),
    );

    // First fetch - should go to GitHub fallback
    let result1 = registry.get_schema("alert-packet", 1000).await;
    assert!(result1.is_ok());
    let schema1_form = result1.unwrap().canonical_form();

    // Second fetch - should come from cache
    let result2 = registry.get_schema("alert-packet", 1000).await;
    assert!(result2.is_ok());
    let schema2_form = result2.unwrap().canonical_form();

    // Both should be the same (cached)
    assert_eq!(
        schema1_form, schema2_form,
        "Cached schema should be identical to first fetch"
    );
}

/// Test that different schema IDs are cached separately
#[tokio::test]
async fn test_schema_registry_multiple_versions() {
    let mut registry = SchemaRegistry::new(
        "http://localhost:9999",
        Some(LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL.to_string()),
    );

    // Try to get schemas with different version IDs
    let result1 = registry.get_schema("alert-packet", 1000).await;
    assert!(
        result1.is_ok(),
        "Schema 1000 should be fetched successfully (error: {:?})",
        result1.err()
    );
    let canonical1 = result1.unwrap().canonical_form();

    let result2 = registry.get_schema("alert-packet", 900).await;
    assert!(
        result2.is_ok(),
        "Schema 900 should be fetched successfully (error: {:?})",
        result2.err()
    );
    let canonical2 = result2.unwrap().canonical_form();

    // These are 2 valid distinct schema versions, so they should differ
    assert_ne!(
        canonical1, canonical2,
        "Different schema versions should yield different canonical forms"
    );
}
