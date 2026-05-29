pub fn parse_positive_usize(s: &str) -> Result<usize, String> {
    let n: usize = s.parse().map_err(|e| format!("not a usize: {e}"))?;
    if n == 0 {
        return Err("must be > 0".to_string());
    }
    Ok(n)
}
