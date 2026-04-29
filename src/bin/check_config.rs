use boom::conf::AppConfig;
use std::error::Error;
use std::path::Path;

fn ensure_required_env() {
    // AppConfig::from_path validates these secrets must be non-empty.
    std::env::set_var("BOOM_DATABASE__PASSWORD", "ci-dummy-db-password");
    std::env::set_var("BOOM_API__AUTH__SECRET_KEY", "ci-dummy-secret-key");
    std::env::set_var("BOOM_API__AUTH__ADMIN_PASSWORD", "ci-dummy-admin-password");
}

fn main() -> Result<(), Box<dyn Error>> {
    ensure_required_env();

    let mut args = std::env::args();
    let _program = args.next();
    let path = match args.next() {
        Some(p) => p,
        None => return Err("usage: check_config {path}".into()),
    };

    if args.next().is_some() {
        return Err("usage: check_config {path}".into());
    }

    let path_obj = Path::new(&path);
    if !path_obj.exists() {
        return Err(format!("config file not found: {}", path).into());
    }

    AppConfig::from_path(&path)?;
    println!("OK: {}", path);
    Ok(())
}
