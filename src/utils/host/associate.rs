use serde::{Deserialize, Serialize};

use super::dlr::{compute_dlr, DlrResult};
use super::ellipse::Ellipse;
use super::error::HostError;
use super::likelihood::{absmag_likelihood, offset_likelihood, redshift_likelihood};
use super::prior;
use super::types::{GalaxyCandidate, HostCandidate, Transient};

/// Configuration for the host association algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociationConfig {
    /// Largest fractional offset (d_DLR) still treated as a plausible host.
    ///
    /// This is the *candidate admission* cutoff, deliberately looser than the
    /// d_DLR a filter would cut on, so that the posterior is normalised over
    /// the full plausible set rather than a pre-truncated one.
    pub max_fractional_offset: f64,
    /// Minimum semi-minor axis in arcsec (floor for tiny or degenerate shapes)
    pub min_b_arcsec: f64,
    /// Maximum number of candidates to return
    pub max_candidates: usize,
    /// Whether to use redshift information in scoring
    pub use_redshift: bool,
    /// Whether to use absolute magnitude in scoring
    pub use_absmag: bool,
}

impl Default for AssociationConfig {
    fn default() -> Self {
        Self {
            max_fractional_offset: 10.0,
            min_b_arcsec: 0.05,
            max_candidates: 10,
            use_redshift: true,
            use_absmag: false,
        }
    }
}

/// Full result of a host galaxy association.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociationResult {
    /// Ranked list of host candidates, best posterior first
    pub candidates: Vec<HostCandidate>,
    /// Probability that none of the candidates is the true host
    pub p_none: f64,
    /// Number of input galaxies considered
    pub n_considered: usize,
}

impl AssociationResult {
    /// Returns the best host candidate, if any.
    pub fn best_host(&self) -> Option<&HostCandidate> {
        self.candidates.first()
    }
}

/// A candidate that survived shape validation and the offset cutoff.
struct Scored {
    index: usize,
    dlr: DlrResult,
    dlr_rank: u32,
    posterior_offset: f64,
    posterior_redshift: f64,
    posterior_absmag: f64,
    posterior: f64,
}

/// Perform probabilistic host galaxy association.
///
/// Given a transient and a list of galaxy candidates, computes DLR-based
/// fractional offsets and Bayesian posterior probabilities for each candidate.
/// Candidates with unusable shapes (non-positive or non-finite axes) are
/// skipped rather than failing the whole association.
pub fn associate_host(
    transient: &Transient,
    candidates: &[GalaxyCandidate],
    config: &AssociationConfig,
) -> Result<AssociationResult, HostError> {
    if candidates.is_empty() {
        return Err(HostError::NoCandidates);
    }

    let mut scored: Vec<Scored> = Vec::new();
    for (index, galaxy) in candidates.iter().enumerate() {
        let Ok(ellipse) = Ellipse::from_candidate(galaxy, config.min_b_arcsec) else {
            continue; // unusable shape - no directional radius to compute
        };

        let dlr = compute_dlr(transient.ra, transient.dec, galaxy.ra, galaxy.dec, &ellipse);

        // Drop non-finite offsets explicitly: a NaN (from non-finite input
        // coordinates) compares false against any bound, so relying on the
        // bound alone to reject it would depend on comparison subtleties.
        if !dlr.fractional_offset.is_finite()
            || dlr.fractional_offset > config.max_fractional_offset
        {
            continue;
        }

        scored.push(Scored {
            index,
            dlr,
            dlr_rank: 0, // assigned below, once sorted by offset
            posterior_offset: 0.0,
            posterior_redshift: 1.0,
            posterior_absmag: 1.0,
            posterior: 0.0,
        });
    }

    if scored.is_empty() {
        return Ok(AssociationResult {
            candidates: Vec::new(),
            p_none: 1.0,
            n_considered: candidates.len(),
        });
    }

    // Rank by fractional offset first, so `dlr_rank` genuinely means "1 = the
    // galaxy this transient sits deepest inside", independent of how the
    // redshift term later reorders the posterior.
    scored.sort_by(|a, b| a.dlr.fractional_offset.total_cmp(&b.dlr.fractional_offset));
    for (i, s) in scored.iter_mut().enumerate() {
        s.dlr_rank = (i + 1) as u32;
    }

    for s in scored.iter_mut() {
        let galaxy = &candidates[s.index];

        s.posterior_offset = offset_likelihood(s.dlr.fractional_offset)
            * prior::offset_prior(s.dlr.fractional_offset, config.max_fractional_offset);

        s.posterior_redshift = if config.use_redshift {
            redshift_likelihood(
                galaxy.redshift,
                galaxy.redshift_err,
                transient.redshift,
                transient.redshift_err,
            )
        } else {
            1.0
        };

        s.posterior_absmag = if config.use_absmag {
            absmag_likelihood(galaxy.mag, galaxy.mag_err, galaxy.redshift)
        } else {
            1.0
        };

        s.posterior = s.posterior_offset * s.posterior_redshift * s.posterior_absmag;
    }

    // Null hypothesis: the host is outside the search radius, too faint to be
    // catalogued, or the transient is genuinely hostless.
    let p_null = prior::p_outside(scored.len()) + prior::p_unobserved() + prior::p_hostless();

    // Normalise over every candidate considered, not just the ones returned,
    // so truncating to `max_candidates` does not inflate the reported
    // posteriors of the survivors.
    let total: f64 = scored.iter().map(|s| s.posterior).sum::<f64>() + p_null;

    let p_none = if total > 0.0 && total.is_finite() {
        (p_null / total).clamp(0.0, 1.0)
    } else {
        1.0
    };

    scored.sort_by(|a, b| b.posterior.total_cmp(&a.posterior));

    let host_candidates: Vec<HostCandidate> = scored
        .iter()
        .take(config.max_candidates)
        .map(|s| HostCandidate {
            galaxy: candidates[s.index].clone(),
            separation_arcsec: s.dlr.separation_arcsec,
            dlr: s.dlr.directional_radius,
            fractional_offset: s.dlr.fractional_offset,
            dlr_rank: s.dlr_rank,
            posterior: if total > 0.0 && total.is_finite() {
                s.posterior / total
            } else {
                0.0
            },
            posterior_offset: s.posterior_offset,
            posterior_redshift: s.posterior_redshift,
            posterior_absmag: s.posterior_absmag,
        })
        .collect();

    Ok(AssociationResult {
        candidates: host_candidates,
        p_none,
        n_considered: candidates.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_galaxy(ra: f64, dec: f64, a: f64, b: f64, pa: f64) -> GalaxyCandidate {
        GalaxyCandidate {
            ra,
            dec,
            a_arcsec: a,
            b_arcsec: b,
            pa_deg: pa,
            redshift: None,
            redshift_err: None,
            dist_mpc: None,
            dist_mpc_method: None,
            mag: None,
            mag_err: None,
            objtype: None,
            objname: None,
            catalog: None,
            shape_from_image: false,
            size_is_isophotal: true,
            diam_survey: None,
            orientation_is_nominal: false,
        }
    }

    #[test]
    fn test_associate_single_nearby() {
        let transient = Transient::new(180.0, 45.0);
        let galaxy = make_galaxy(180.0, 45.0 + 1.0 / 3600.0, 5.0, 3.0, 0.0);

        let result = associate_host(&transient, &[galaxy], &AssociationConfig::default()).unwrap();

        assert_eq!(result.candidates.len(), 1);
        assert!(result.candidates[0].posterior > 0.5);
        assert!(result.p_none < 0.5);
    }

    #[test]
    fn test_associate_empty() {
        let transient = Transient::new(180.0, 45.0);
        assert!(associate_host(&transient, &[], &AssociationConfig::default()).is_err());
    }

    #[test]
    fn test_associate_ranking() {
        let transient = Transient::new(180.0, 45.0);
        let g1 = make_galaxy(180.0, 45.0 + 0.5 / 3600.0, 5.0, 3.0, 0.0);
        let g2 = make_galaxy(180.0, 45.0 + 10.0 / 3600.0, 5.0, 3.0, 0.0);

        let result = associate_host(&transient, &[g1, g2], &AssociationConfig::default()).unwrap();

        assert_eq!(result.candidates.len(), 2);
        assert!(result.candidates[0].posterior > result.candidates[1].posterior);
        assert_eq!(result.candidates[0].dlr_rank, 1);
        assert_eq!(result.candidates[1].dlr_rank, 2);
    }

    #[test]
    fn test_associate_with_redshift() {
        let transient = Transient::new(180.0, 45.0).with_redshift(0.05, 0.001);

        let mut g1 = make_galaxy(180.0, 45.0 + 2.0 / 3600.0, 5.0, 3.0, 0.0);
        g1.redshift = Some(0.05);
        g1.redshift_err = Some(0.001);

        let mut g2 = make_galaxy(180.0, 45.0 + 2.0 / 3600.0, 5.0, 3.0, 0.0);
        g2.redshift = Some(0.5);
        g2.redshift_err = Some(0.001);

        let config = AssociationConfig {
            use_redshift: true,
            ..Default::default()
        };
        let result = associate_host(&transient, &[g1, g2], &config).unwrap();

        assert!(result.candidates[0].posterior > result.candidates[1].posterior);
        assert_close!(
            result.candidates[0].galaxy.redshift.unwrap(),
            0.05,
            epsilon = 0.001
        );
    }

    #[test]
    fn test_posteriors_sum_to_one() {
        let transient = Transient::new(180.0, 45.0);
        let galaxies: Vec<GalaxyCandidate> = (1..=5)
            .map(|i| make_galaxy(180.0, 45.0 + (i as f64) / 3600.0, 4.0, 2.0, 0.0))
            .collect();

        let result = associate_host(&transient, &galaxies, &AssociationConfig::default()).unwrap();

        let sum: f64 = result.candidates.iter().map(|c| c.posterior).sum::<f64>() + result.p_none;
        assert_close!(sum, 1.0, epsilon = 1e-6);
    }

    #[test]
    fn test_dlr_rank_tracks_offset_not_posterior() {
        // A redshift-discrepant galaxy sits closest in d_DLR, so it must keep
        // dlr_rank 1 even though the posterior demotes it to last place.
        let transient = Transient::new(180.0, 45.0).with_redshift(0.05, 0.001);

        let mut near_wrong_z = make_galaxy(180.0, 45.0 + 1.0 / 3600.0, 5.0, 3.0, 0.0);
        near_wrong_z.redshift = Some(0.5);
        near_wrong_z.redshift_err = Some(0.001);

        let mut far_right_z = make_galaxy(180.0, 45.0 + 4.0 / 3600.0, 5.0, 3.0, 0.0);
        far_right_z.redshift = Some(0.05);
        far_right_z.redshift_err = Some(0.001);

        let result = associate_host(
            &transient,
            &[near_wrong_z, far_right_z],
            &AssociationConfig::default(),
        )
        .unwrap();

        // Posterior order: the redshift-matching galaxy wins.
        assert_close!(
            result.candidates[0].galaxy.redshift.unwrap(),
            0.05,
            epsilon = 1e-9
        );
        // But the rank still reflects which galaxy the transient is deepest in.
        assert_eq!(result.candidates[0].dlr_rank, 2);
        assert_eq!(result.candidates[1].dlr_rank, 1);
    }

    #[test]
    fn test_skips_unusable_shapes_but_keeps_the_rest() {
        let transient = Transient::new(180.0, 45.0);
        let good = make_galaxy(180.0, 45.0 + 1.0 / 3600.0, 5.0, 3.0, 0.0);
        let no_shape = make_galaxy(180.0, 45.0 + 1.0 / 3600.0, 0.0, 0.0, 0.0);
        let nan_shape = make_galaxy(180.0, 45.0 + 1.0 / 3600.0, f64::NAN, f64::NAN, 0.0);

        let result = associate_host(
            &transient,
            &[no_shape, good, nan_shape],
            &AssociationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.n_considered, 3);
        assert_close!(result.candidates[0].galaxy.a_arcsec, 5.0);
    }

    #[test]
    fn test_all_candidates_beyond_cutoff() {
        let transient = Transient::new(180.0, 45.0);
        // 100 arcsec away from a 1 arcsec galaxy -> d_DLR = 100, far past the cutoff
        let far = make_galaxy(180.0, 45.0 + 100.0 / 3600.0, 1.0, 1.0, 0.0);

        let result = associate_host(&transient, &[far], &AssociationConfig::default()).unwrap();

        assert!(result.candidates.is_empty());
        assert_close!(result.p_none, 1.0);
        assert_eq!(result.n_considered, 1);
    }

    #[test]
    fn test_truncation_does_not_inflate_posteriors() {
        let transient = Transient::new(180.0, 45.0);
        let galaxies: Vec<GalaxyCandidate> = (1..=8)
            .map(|i| make_galaxy(180.0, 45.0 + (i as f64) / 3600.0, 4.0, 2.0, 0.0))
            .collect();

        let config = AssociationConfig {
            max_candidates: 3,
            ..Default::default()
        };
        let result = associate_host(&transient, &galaxies, &config).unwrap();

        assert_eq!(result.candidates.len(), 3);
        // Normalised over all 8, so the returned three must sum to well under 1.
        let sum: f64 = result.candidates.iter().map(|c| c.posterior).sum();
        assert!(sum + result.p_none < 1.0);
        assert!(result.candidates.iter().all(|c| c.posterior <= 1.0));
    }
}
