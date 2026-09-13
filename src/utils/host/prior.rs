/// Uniform prior on the fractional offset: 1/max_offset over [0, max_offset].
pub fn offset_prior(fractional_offset: f64, max_offset: f64) -> f64 {
    if max_offset <= 0.0 {
        return 0.0;
    }
    if fractional_offset >= 0.0 && fractional_offset <= max_offset {
        1.0 / max_offset
    } else {
        0.0
    }
}

/// Prior probability that the true host lies outside the search radius.
pub fn p_outside(n_candidates: usize) -> f64 {
    if n_candidates == 0 {
        0.5
    } else {
        0.01
    }
}

/// Prior probability that the true host is too faint to be in the catalog.
pub fn p_unobserved() -> f64 {
    0.01
}

/// Prior probability that the transient is genuinely hostless.
pub fn p_hostless() -> f64 {
    0.005
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_prior_in_range() {
        assert_close!(offset_prior(3.0, 10.0), 0.1);
    }

    #[test]
    fn test_offset_prior_at_boundary() {
        assert_close!(offset_prior(10.0, 10.0), 0.1);
        assert_close!(offset_prior(0.0, 10.0), 0.1);
    }

    #[test]
    fn test_offset_prior_out_of_range() {
        assert_close!(offset_prior(11.0, 10.0), 0.0);
        assert_close!(offset_prior(-1.0, 10.0), 0.0);
    }

    #[test]
    fn test_offset_prior_degenerate_max() {
        assert_close!(offset_prior(0.0, 0.0), 0.0);
        assert_close!(offset_prior(1.0, -5.0), 0.0);
    }

    #[test]
    fn test_p_outside_no_candidates() {
        assert_close!(p_outside(0), 0.5);
    }

    #[test]
    fn test_p_outside_with_candidates() {
        assert_close!(p_outside(5), 0.01);
    }
}
