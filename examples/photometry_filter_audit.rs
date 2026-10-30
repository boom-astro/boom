//! Read-only inventory of exported active filters; no service connections.
use anyhow::{ensure, Result};
use serde_json::json;

fn main() -> Result<()> {
    let paths: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        !paths.is_empty(),
        "usage: photometry_filter_audit PIPELINE_OR_FILTER_EXPORT_JSON..."
    );
    let mut reports = Vec::new();
    for path in paths {
        let filters = boom::photometry::audit::inventory(std::path::Path::new(&path))?;
        reports.push(json!({"file":path,"filters":filters}));
    }
    println!("{}", serde_json::to_string_pretty(&reports)?);
    Ok(())
}
