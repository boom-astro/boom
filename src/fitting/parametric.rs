use std::collections::HashMap;

use argmin::core::{CostFunction, Error as ArgminError, Executor, State};
use argmin::solver::particleswarm::ParticleSwarm;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use super::common::{finite_or_none, BandData};

// Physical constants (CGS)
const MSUN_CGS: f64 = 1.989e33;
const C_CGS: f64 = 2.998e10;
const SECS_PER_DAY: f64 = 86400.0;

/// Sigma inflation factor for mean-field VI posteriors.
const SIGMA_INFLATION_FACTOR: f64 = 4.0;

// ---------------------------------------------------------------------------
// Manual Adam optimizer
// ---------------------------------------------------------------------------

struct ManualAdam {
    m: Vec<f64>,
    v: Vec<f64>,
    lr: f64,
    beta1: f64,
    beta2: f64,
    eps: f64,
    t: usize,
}

impl ManualAdam {
    fn new(n_params: usize, lr: f64) -> Self {
        Self {
            m: vec![0.0; n_params],
            v: vec![0.0; n_params],
            lr,
            beta1: 0.9,
            beta2: 0.999,
            eps: 1e-8,
            t: 0,
        }
    }

    fn step(&mut self, params: &mut [f64], grads: &[f64]) {
        self.t += 1;
        let bc1 = 1.0 - self.beta1.powi(self.t as i32);
        let bc2 = 1.0 - self.beta2.powi(self.t as i32);
        for i in 0..params.len() {
            let g = grads[i];
            if !g.is_finite() {
                continue;
            }
            self.m[i] = self.beta1 * self.m[i] + (1.0 - self.beta1) * g;
            self.v[i] = self.beta2 * self.v[i] + (1.0 - self.beta2) * g * g;
            let m_hat = self.m[i] / bc1;
            let v_hat = self.v[i] / bc2;
            params[i] -= self.lr * m_hat / (v_hat.sqrt() + self.eps);
        }
    }
}

// ---------------------------------------------------------------------------
// Model definitions
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum SviModelName {
    Bazin,
    Villar,
    MetzgerKN,
    Tde,
    Arnett,
    Magnetar,
    ShockCooling,
    Afterglow,
}

impl std::fmt::Display for SviModelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SviModelName::Bazin => write!(f, "Bazin"),
            SviModelName::Villar => write!(f, "Villar"),
            SviModelName::MetzgerKN => write!(f, "MetzgerKN"),
            SviModelName::Tde => write!(f, "Tde"),
            SviModelName::Arnett => write!(f, "Arnett"),
            SviModelName::Magnetar => write!(f, "Magnetar"),
            SviModelName::ShockCooling => write!(f, "ShockCooling"),
            SviModelName::Afterglow => write!(f, "Afterglow"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SviModel {
    Bazin,
    Villar,
    MetzgerKN,
    Tde,
    Arnett,
    Magnetar,
    ShockCooling,
    Afterglow,
}

impl SviModel {
    fn n_params(self) -> usize {
        match self {
            SviModel::Bazin => 6,
            SviModel::Villar => 7,
            SviModel::MetzgerKN => 5,
            SviModel::Tde => 7,
            SviModel::Arnett => 5,
            SviModel::Magnetar => 5,
            SviModel::ShockCooling => 5,
            SviModel::Afterglow => 6,
        }
    }

    fn sigma_extra_idx(self) -> usize {
        match self {
            SviModel::Bazin => 5,
            SviModel::Villar => 6,
            SviModel::MetzgerKN => 4,
            SviModel::Tde => 6,
            SviModel::Arnett => 4,
            SviModel::Magnetar => 4,
            SviModel::ShockCooling => 4,
            SviModel::Afterglow => 5,
        }
    }

    fn t0_idx(self) -> usize {
        match self {
            SviModel::Bazin => 2,
            SviModel::Villar => 3,
            SviModel::MetzgerKN => 3,
            SviModel::Tde => 2,
            SviModel::Arnett => 1,
            SviModel::Magnetar => 1,
            SviModel::ShockCooling => 1,
            SviModel::Afterglow => 1,
        }
    }

    fn is_sequential(self) -> bool {
        matches!(self, SviModel::MetzgerKN)
    }

    fn to_name(self) -> SviModelName {
        match self {
            SviModel::Bazin => SviModelName::Bazin,
            SviModel::Villar => SviModelName::Villar,
            SviModel::MetzgerKN => SviModelName::MetzgerKN,
            SviModel::Tde => SviModelName::Tde,
            SviModel::Arnett => SviModelName::Arnett,
            SviModel::Magnetar => SviModelName::Magnetar,
            SviModel::ShockCooling => SviModelName::ShockCooling,
            SviModel::Afterglow => SviModelName::Afterglow,
        }
    }
}

// ---------------------------------------------------------------------------
// Model evaluation functions
// ---------------------------------------------------------------------------

#[inline]
fn bazin_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let b = params[1];
    let t0 = params[2];
    let tau_rise = params[3].exp();
    let tau_fall = params[4].exp();
    let dt = t - t0;
    let e_fall = (-dt / tau_fall).exp();
    let sig = 1.0 / (1.0 + (-dt / tau_rise).exp());
    a * e_fall * sig + b
}

#[inline]
fn bazin_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let t0 = params[2];
    let tau_rise = params[3].exp();
    let tau_fall = params[4].exp();
    let dt = t - t0;
    let e_fall = (-dt / tau_fall).exp();
    let sig = 1.0 / (1.0 + (-dt / tau_rise).exp());
    let base = a * e_fall * sig;
    out[0] = base;
    out[1] = 1.0;
    out[2] = base * (1.0 / tau_fall - (1.0 - sig) / tau_rise);
    out[3] = -base * (1.0 - sig) * dt / tau_rise;
    out[4] = base * dt / tau_fall;
    out[5] = 0.0;
}

#[inline]
fn villar_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let beta = params[1];
    let gamma = params[2].exp();
    let t0 = params[3];
    let tau_rise = params[4].exp();
    let tau_fall = params[5].exp();
    let phase = t - t0;
    let sig_rise = 1.0 / (1.0 + (-phase / tau_rise).exp());
    let k = 10.0;
    let w = 1.0 / (1.0 + (-k * (phase - gamma)).exp());
    let piece_left = 1.0 - beta * phase;
    let piece_right = (1.0 - beta * gamma) * ((gamma - phase) / tau_fall).exp();
    let piece = (1.0 - w) * piece_left + w * piece_right;
    a * sig_rise * piece
}

#[inline]
fn villar_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let beta = params[1];
    let gamma = params[2].exp();
    let t0 = params[3];
    let tau_rise = params[4].exp();
    let tau_fall = params[5].exp();
    let phase = t - t0;
    let k = 10.0;
    let sig_rise = 1.0 / (1.0 + (-phase / tau_rise).exp());
    let w = 1.0 / (1.0 + (-k * (phase - gamma)).exp());
    let piece_left = 1.0 - beta * phase;
    let e_decay = ((gamma - phase) / tau_fall).exp();
    let piece_right = (1.0 - beta * gamma) * e_decay;
    let piece = (1.0 - w) * piece_left + w * piece_right;
    let flux = a * sig_rise * piece;

    out[0] = flux;

    let d_pl_dbeta = -phase;
    let d_pr_dbeta = -gamma * e_decay;
    out[1] = a * sig_rise * ((1.0 - w) * d_pl_dbeta + w * d_pr_dbeta);

    let dw_dgamma = -k * w * (1.0 - w);
    let dw_dloggamma = dw_dgamma * gamma;
    let dpr_dgamma = e_decay * (-beta + (1.0 - beta * gamma) / tau_fall);
    let dpr_dloggamma = dpr_dgamma * gamma;
    let d_piece_dloggamma = dw_dloggamma * (piece_right - piece_left) + w * dpr_dloggamma;
    out[2] = a * sig_rise * d_piece_dloggamma;

    let dsig_dphase = sig_rise * (1.0 - sig_rise) / tau_rise;
    let dsig_dt0 = -dsig_dphase;
    let dw_dphase = k * w * (1.0 - w);
    let dw_dt0 = -dw_dphase;
    let dpl_dt0 = beta;
    let dpr_dt0 = (1.0 - beta * gamma) * e_decay / tau_fall;
    let d_piece_dt0 = dw_dt0 * (piece_right - piece_left) + (1.0 - w) * dpl_dt0 + w * dpr_dt0;
    out[3] = a * (dsig_dt0 * piece + sig_rise * d_piece_dt0);

    out[4] = a * piece * sig_rise * (1.0 - sig_rise) * (-phase / tau_rise);

    let d_pr_dlogtf = piece_right * (phase - gamma) / tau_fall;
    out[5] = a * sig_rise * w * d_pr_dlogtf;

    out[6] = 0.0;
}

#[inline]
fn tde_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let b = params[1];
    let t0 = params[2];
    let tau_rise = params[3].exp();
    let tau_fall = params[4].exp();
    let alpha = params[5];
    let phase = t - t0;
    let sig = 1.0 / (1.0 + (-phase / tau_rise).exp());
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let w = 1.0 + phase_soft / tau_fall;
    let decay = w.powf(-alpha);
    a * sig * decay + b
}

#[inline]
fn tde_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let t0 = params[2];
    let tau_rise = params[3].exp();
    let tau_fall = params[4].exp();
    let alpha = params[5];
    let phase = t - t0;
    let sig = 1.0 / (1.0 + (-phase / tau_rise).exp());
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let sig_phase = phase.exp() / (1.0 + phase.exp());
    let w = 1.0 + phase_soft / tau_fall;
    let decay = w.powf(-alpha);
    let base = a * sig * decay;

    out[0] = base;
    out[1] = 1.0;
    let dsig_dt0 = -sig * (1.0 - sig) / tau_rise;
    let ddecay_dt0 = alpha * w.powf(-alpha - 1.0) * sig_phase / tau_fall;
    out[2] = a * (dsig_dt0 * decay + sig * ddecay_dt0);
    out[3] = a * decay * (-sig * (1.0 - sig) * phase / tau_rise);
    out[4] = a * sig * alpha * w.powf(-alpha - 1.0) * phase_soft / tau_fall;
    out[5] = a * sig * (-w.ln()) * decay;
    out[6] = 0.0;
}

#[inline]
fn arnett_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let t0 = params[1];
    let tau_m = params[2].exp();
    let logit_f = params[3];
    let phase = t - t0;
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let f = 1.0 / (1.0 + (-logit_f).exp());
    const TAU_NI: f64 = 8.8;
    const TAU_CO: f64 = 111.3;
    let e_ni = (-phase_soft / TAU_NI).exp();
    let e_co = (-phase_soft / TAU_CO).exp();
    let heat = f * e_ni + (1.0 - f) * e_co;
    let x = phase_soft / tau_m;
    let trap = 1.0 - (-x * x).exp();
    a * heat * trap
}

#[inline]
fn arnett_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let t0 = params[1];
    let tau_m = params[2].exp();
    let logit_f = params[3];
    let phase = t - t0;
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let sig_p = phase.exp() / (1.0 + phase.exp());
    let f = 1.0 / (1.0 + (-logit_f).exp());
    const TAU_NI: f64 = 8.8;
    const TAU_CO: f64 = 111.3;
    let e_ni = (-phase_soft / TAU_NI).exp();
    let e_co = (-phase_soft / TAU_CO).exp();
    let heat = f * e_ni + (1.0 - f) * e_co;
    let x = phase_soft / tau_m;
    let exp_x2 = (-x * x).exp();
    let trap = 1.0 - exp_x2;
    let flux = a * heat * trap;

    out[0] = flux;
    let dheat_dps = -f * e_ni / TAU_NI - (1.0 - f) * e_co / TAU_CO;
    let dtrap_dps = 2.0 * phase_soft * exp_x2 / (tau_m * tau_m);
    out[1] = a * (-sig_p) * (dheat_dps * trap + heat * dtrap_dps);
    out[2] = -2.0 * a * heat * exp_x2 * x * x;
    out[3] = a * trap * (e_ni - e_co) * f * (1.0 - f);
    out[4] = 0.0;
}

#[inline]
fn magnetar_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let t0 = params[1];
    let tau_sd = params[2].exp();
    let tau_diff = params[3].exp();
    let phase = t - t0;
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let w = 1.0 + phase_soft / tau_sd;
    let spindown = w.powi(-2);
    let x = phase_soft / tau_diff;
    let trap = 1.0 - (-x * x).exp();
    a * spindown * trap
}

#[inline]
fn magnetar_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let t0 = params[1];
    let tau_sd = params[2].exp();
    let tau_diff = params[3].exp();
    let phase = t - t0;
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let sig_p = phase.exp() / (1.0 + phase.exp());
    let w = 1.0 + phase_soft / tau_sd;
    let spindown = w.powi(-2);
    let x = phase_soft / tau_diff;
    let exp_x2 = (-x * x).exp();
    let trap = 1.0 - exp_x2;
    let flux = a * spindown * trap;

    out[0] = flux;
    let dspindown_dps = -2.0 * w.powi(-3) / tau_sd;
    let dtrap_dps = 2.0 * phase_soft * exp_x2 / (tau_diff * tau_diff);
    out[1] = a * (-sig_p) * (dspindown_dps * trap + spindown * dtrap_dps);
    out[2] = a * trap * 2.0 * phase_soft * w.powi(-3) / tau_sd;
    out[3] = -2.0 * a * spindown * exp_x2 * x * x;
    out[4] = 0.0;
}

#[inline]
fn shockcooling_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let t0 = params[1];
    let n = params[2];
    let tau_tr = params[3].exp();
    let phase = t - t0;
    let sig5 = 1.0 / (1.0 + (-phase * 5.0).exp());
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let cooling = phase_soft.powf(-n);
    let ratio = phase_soft / tau_tr;
    let cutoff = (-ratio * ratio).exp();
    a * sig5 * cooling * cutoff
}

#[inline]
fn shockcooling_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let t0 = params[1];
    let n = params[2];
    let tau_tr = params[3].exp();
    let phase = t - t0;
    let sig5 = 1.0 / (1.0 + (-phase * 5.0).exp());
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let sig_p = phase.exp() / (1.0 + phase.exp());
    let cooling = phase_soft.powf(-n);
    let ratio = phase_soft / tau_tr;
    let cutoff = (-ratio * ratio).exp();
    let base = cooling * cutoff;
    let flux = a * sig5 * base;

    out[0] = flux;
    out[1] = a
        * base
        * (-5.0 * sig5 * (1.0 - sig5)
            + sig5 * sig_p * (n / phase_soft + 2.0 * phase_soft / (tau_tr * tau_tr)));
    out[2] = -flux * phase_soft.ln();
    out[3] = flux * 2.0 * ratio * ratio;
    out[4] = 0.0;
}

#[inline]
fn afterglow_flux_eval(params: &[f64], t: f64) -> f64 {
    let a = params[0].exp();
    let t0 = params[1];
    let t_b = params[2].exp();
    let alpha1 = params[3];
    let alpha2 = params[4];
    let phase = t - t0;
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let r = phase_soft / t_b;
    let ln_r = r.ln();
    let u1 = (2.0 * alpha1 * ln_r).exp();
    let u2 = (2.0 * alpha2 * ln_r).exp();
    let u = u1 + u2;
    a * u.powf(-0.5)
}

#[inline]
fn afterglow_flux_grad(params: &[f64], t: f64, out: &mut [f64]) {
    let a = params[0].exp();
    let t0 = params[1];
    let t_b = params[2].exp();
    let alpha1 = params[3];
    let alpha2 = params[4];
    let phase = t - t0;
    let phase_soft = (1.0 + phase.exp()).ln() + 1e-6;
    let sig_p = phase.exp() / (1.0 + phase.exp());
    let r = phase_soft / t_b;
    let ln_r = r.ln();
    let u1 = (2.0 * alpha1 * ln_r).exp();
    let u2 = (2.0 * alpha2 * ln_r).exp();
    let u = u1 + u2;
    let flux = a * u.powf(-0.5);

    out[0] = flux;
    let du_dps = (2.0 * alpha1 * u1 + 2.0 * alpha2 * u2) / phase_soft;
    let dflux_dps = a * (-0.5) * u.powf(-1.5) * du_dps;
    out[1] = dflux_dps * (-sig_p);
    let du_dlog_tb = -(2.0 * alpha1 * u1 + 2.0 * alpha2 * u2);
    out[2] = a * (-0.5) * u.powf(-1.5) * du_dlog_tb;
    out[3] = a * (-0.5) * u.powf(-1.5) * 2.0 * ln_r * u1;
    out[4] = a * (-0.5) * u.powf(-1.5) * 2.0 * ln_r * u2;
    out[5] = 0.0;
}

#[inline]
fn eval_model(model: SviModel, params: &[f64], t: f64) -> f64 {
    match model {
        SviModel::Bazin => bazin_flux_eval(params, t),
        SviModel::Villar => villar_flux_eval(params, t),
        SviModel::Tde => tde_flux_eval(params, t),
        SviModel::Arnett => arnett_flux_eval(params, t),
        SviModel::Magnetar => magnetar_flux_eval(params, t),
        SviModel::ShockCooling => shockcooling_flux_eval(params, t),
        SviModel::Afterglow => afterglow_flux_eval(params, t),
        SviModel::MetzgerKN => panic!("MetzgerKN requires batch evaluation"),
    }
}

#[inline]
fn eval_model_grad(model: SviModel, params: &[f64], t: f64, out: &mut [f64]) {
    match model {
        SviModel::Bazin => bazin_flux_grad(params, t, out),
        SviModel::Villar => villar_flux_grad(params, t, out),
        SviModel::Tde => tde_flux_grad(params, t, out),
        SviModel::Arnett => arnett_flux_grad(params, t, out),
        SviModel::Magnetar => magnetar_flux_grad(params, t, out),
        SviModel::ShockCooling => shockcooling_flux_grad(params, t, out),
        SviModel::Afterglow => afterglow_flux_grad(params, t, out),
        SviModel::MetzgerKN => panic!("MetzgerKN requires batch evaluation"),
    }
}

// ---------------------------------------------------------------------------
// Metzger 1-zone kilonova
// ---------------------------------------------------------------------------

fn metzger_kn_eval_batch(params: &[f64], obs_times: &[f64]) -> Vec<f64> {
    let m_ej = 10f64.powf(params[0]) * MSUN_CGS;
    let v_ej = 10f64.powf(params[1]) * C_CGS;
    let kappa_r = 10f64.powf(params[2]);
    let t0 = params[3];

    let phases: Vec<f64> = obs_times.iter().map(|&t| t - t0).collect();
    let phase_max = phases.iter().cloned().fold(0.01f64, f64::max);
    if phase_max <= 0.01 {
        return vec![0.0; obs_times.len()];
    }

    let n_grid: usize = 200;
    let log_t_min = 0.01f64.ln();
    let log_t_max = (phase_max * 1.05).ln();
    let grid_t_day: Vec<f64> = (0..n_grid)
        .map(|i| (log_t_min + (log_t_max - log_t_min) * i as f64 / (n_grid - 1) as f64).exp())
        .collect();

    let ye: f64 = 0.1;
    let xn0: f64 = 1.0 - 2.0 * ye;

    let scale: f64 = 1e40;
    let e0 = 0.5 * m_ej * v_ej * v_ej;
    let mut e_th = e0 / scale;
    let mut e_kin = e0 / scale;
    let mut v = v_ej;
    let mut r = grid_t_day[0] * SECS_PER_DAY * v;

    let mut grid_lrad: Vec<f64> = Vec::with_capacity(n_grid);

    for i in 0..n_grid {
        let t_day = grid_t_day[i];
        let t_sec = t_day * SECS_PER_DAY;

        let eth_factor = 0.34 * t_day.powf(0.74);
        let eth = 0.36
            * ((-0.56 * t_day).exp()
                + if eth_factor > 1e-10 {
                    (1.0 + eth_factor).ln() / eth_factor
                } else {
                    1.0
                });

        let xn = xn0 * (-t_sec / 900.0).exp();
        let eps_neutron = 3.2e14 * xn;
        let time_term = (0.5 - ((t_sec - 1.3) / 0.11).atan() / std::f64::consts::PI).max(1e-30);
        let eps_rp = 2e18 * eth * time_term.powf(1.3);
        let l_heat = m_ej * (eps_neutron + eps_rp) / scale;

        let xr = 1.0 - xn0;
        let xn_decayed = xn0 - xn;
        let kappa_eff = 0.4 * xn_decayed + kappa_r * xr;

        let t_diff =
            3.0 * kappa_eff * m_ej / (4.0 * std::f64::consts::PI * C_CGS * v * t_sec) + r / C_CGS;

        let l_rad = if e_th > 0.0 && t_diff > 0.0 {
            e_th / t_diff
        } else {
            0.0
        };
        grid_lrad.push(l_rad);

        let l_pdv = if r > 0.0 { e_th * v / r } else { 0.0 };

        if i < n_grid - 1 {
            let dt_sec = (grid_t_day[i + 1] - grid_t_day[i]) * SECS_PER_DAY;
            e_th += (l_heat - l_pdv - l_rad) * dt_sec;
            if e_th < 0.0 {
                e_th = 0.0;
            }
            e_kin += l_pdv * dt_sec;
            v = (2.0 * e_kin * scale / m_ej).sqrt().min(C_CGS);
            r += v * dt_sec;
        }
    }

    let l_peak = grid_lrad.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if l_peak <= 0.0 || !l_peak.is_finite() {
        return vec![0.0; obs_times.len()];
    }
    let grid_norm: Vec<f64> = grid_lrad.iter().map(|l| l / l_peak).collect();

    phases
        .iter()
        .map(|&phase| {
            if phase <= 0.0 {
                return 0.0;
            }
            if phase <= grid_t_day[0] {
                return grid_norm[0];
            }
            if phase >= grid_t_day[n_grid - 1] {
                return *grid_norm.last().unwrap();
            }
            let idx = grid_t_day
                .partition_point(|&gt| gt < phase)
                .min(n_grid - 1)
                .max(1);
            let frac = (phase - grid_t_day[idx - 1]) / (grid_t_day[idx] - grid_t_day[idx - 1]);
            grid_norm[idx - 1] + frac * (grid_norm[idx] - grid_norm[idx - 1])
        })
        .collect()
}

#[inline]
fn eval_model_batch(model: SviModel, params: &[f64], times: &[f64]) -> Vec<f64> {
    if model.is_sequential() {
        metzger_kn_eval_batch(params, times)
    } else {
        times
            .iter()
            .map(|&t| eval_model(model, params, t))
            .collect()
    }
}

fn metzger_kn_grad_batch_into(params: &[f64], times: &[f64], out: &mut [f64]) {
    let n_times = times.len();
    let n_params = 5;
    let n_phys = 4;
    let base = metzger_kn_eval_batch(params, times);
    let eps = 1e-5;
    for v in out.iter_mut() {
        *v = 0.0;
    }
    for j in 0..n_phys {
        let mut p_plus = params.to_vec();
        p_plus[j] += eps;
        let f_plus = metzger_kn_eval_batch(&p_plus, times);
        for i in 0..n_times {
            out[i * n_params + j] = (f_plus[i] - base[i]) / eps;
        }
    }
}

// ---------------------------------------------------------------------------
// PSO cost function
// ---------------------------------------------------------------------------

#[inline]
fn erf_approx(x: f64) -> f64 {
    let a = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * a);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let val = 1.0 - poly * (-a * a).exp();
    if x >= 0.0 {
        val
    } else {
        -val
    }
}

#[inline]
fn log_normal_cdf(x: f64) -> f64 {
    if x > 8.0 {
        return 0.0;
    }
    if x < -30.0 {
        return -0.5 * x * x - 0.5 * (2.0 * std::f64::consts::PI).ln() - (-x).ln();
    }
    let z = -x * std::f64::consts::FRAC_1_SQRT_2;
    let t = 1.0 / (1.0 + 0.3275911 * z.abs());
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let erfc_z = poly * (-z * z).exp();
    let phi = if z >= 0.0 {
        0.5 * erfc_z
    } else {
        1.0 - 0.5 * erfc_z
    };
    (phi.max(1e-300)).ln()
}

#[derive(Clone)]
struct PsoCost<'a> {
    times: &'a [f64],
    flux: &'a [f64],
    flux_err: &'a [f64],
    is_upper: &'a [bool],
    upper_flux: &'a [f64],
    model: SviModel,
}

impl CostFunction for PsoCost<'_> {
    type Param = Vec<f64>;
    type Output = f64;

    fn cost(&self, p: &Self::Param) -> Result<Self::Output, ArgminError> {
        let se_idx = self.model.sigma_extra_idx();
        let sigma_extra = p[se_idx].exp();
        let sigma_extra_sq = sigma_extra * sigma_extra;
        let mut preds = eval_model_batch(self.model, p, self.times);

        if self.model == SviModel::MetzgerKN {
            let max_pred = preds
                .iter()
                .zip(self.is_upper.iter())
                .filter(|(_, is_up)| !**is_up)
                .map(|(p, _)| *p)
                .fold(f64::NEG_INFINITY, f64::max);
            if max_pred > 1e-10 && max_pred.is_finite() {
                let scale = (1.0 / max_pred).clamp(0.1, 10.0);
                for pred in preds.iter_mut() {
                    *pred *= scale;
                }
            }
        }

        let n = self.times.len().max(1) as f64;
        let mut neg_ll = 0.0;
        for i in 0..self.times.len() {
            let pred = preds[i];
            if !pred.is_finite() {
                return Ok(1e99);
            }
            let total_var = self.flux_err[i] * self.flux_err[i] + sigma_extra_sq + 1e-10;
            if self.is_upper[i] {
                let z = (self.upper_flux[i] - pred) / total_var.sqrt();
                neg_ll -= log_normal_cdf(z);
            } else {
                let diff = pred - self.flux[i];
                neg_ll += diff * diff / total_var + total_var.ln();
            }
        }
        Ok(neg_ll / n)
    }
}

// ---------------------------------------------------------------------------
// Profile likelihood for t0
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct FixedT0PsoCost<'a> {
    inner: PsoCost<'a>,
    t0: f64,
    t0_idx: usize,
}

impl CostFunction for FixedT0PsoCost<'_> {
    type Param = Vec<f64>;
    type Output = f64;

    fn cost(&self, p: &Self::Param) -> Result<Self::Output, ArgminError> {
        let mut full = Vec::with_capacity(p.len() + 1);
        for (i, &val) in p.iter().enumerate() {
            if i == self.t0_idx {
                full.push(self.t0);
            }
            full.push(val);
        }
        if self.t0_idx >= p.len() {
            full.push(self.t0);
        }
        self.inner.cost(&full)
    }
}

fn pso_bounds(model: SviModel) -> (Vec<f64>, Vec<f64>) {
    match model {
        SviModel::Bazin => (
            vec![-3.0, -1.0, -100.0, -2.0, -2.0, -5.0],
            vec![3.0, 1.0, 100.0, 5.0, 6.0, 0.0],
        ),
        SviModel::Villar => (
            vec![-3.0, -0.05, -3.0, -100.0, -2.0, -2.0, -5.0],
            vec![3.0, 0.1, 5.0, 100.0, 5.0, 7.0, 0.0],
        ),
        SviModel::MetzgerKN => (
            vec![-3.0, -2.0, -1.0, -2.0, -5.0],
            vec![-0.5, -0.5, 2.0, 1.0, 0.0],
        ),
        SviModel::Tde => (
            vec![-3.0, -1.0, -100.0, -2.0, -1.0, 0.5, -5.0],
            vec![3.0, 1.0, 100.0, 5.0, 6.0, 4.0, 0.0],
        ),
        SviModel::Arnett => (
            vec![-3.0, -100.0, 0.5, -3.0, -5.0],
            vec![3.0, 100.0, 4.5, 3.0, 0.0],
        ),
        SviModel::Magnetar => (
            vec![-3.0, -100.0, 0.0, 0.5, -5.0],
            vec![3.0, 100.0, 6.0, 4.5, 0.0],
        ),
        SviModel::ShockCooling => (
            vec![-3.0, -100.0, 0.1, -1.0, -5.0],
            vec![3.0, 100.0, 3.0, 4.0, 0.0],
        ),
        SviModel::Afterglow => (
            vec![-3.0, -100.0, -2.0, -2.0, 0.5, -5.0],
            vec![3.0, 100.0, 6.0, 3.0, 5.0, 0.0],
        ),
    }
}

fn pso_bounds_no_t0(model: SviModel) -> (Vec<f64>, Vec<f64>) {
    let (mut lower, mut upper) = pso_bounds(model);
    let idx = model.t0_idx();
    lower.remove(idx);
    upper.remove(idx);
    (lower, upper)
}

fn linspace(start: f64, end: f64, n: usize) -> Vec<f64> {
    if n <= 1 {
        return vec![start];
    }
    let step = (end - start) / (n - 1) as f64;
    (0..n).map(|i| start + i as f64 * step).collect()
}

fn profile_t0_search(
    data: &BandFitData,
    model: SviModel,
    pilot_params: &[f64],
    pilot_cost: f64,
) -> (Vec<f64>, f64) {
    let t0_idx = model.t0_idx();
    let pilot_t0 = pilot_params[t0_idx];
    let t_first = data.times.iter().cloned().fold(f64::INFINITY, f64::min);
    let t0_min = (pilot_t0 - 5.0).max(t_first - 10.0);
    let t0_max = pilot_t0 + 5.0;

    let run_at_t0 = |t0: f64| -> (Vec<f64>, f64) {
        let (lower, upper) = pso_bounds_no_t0(model);
        let inner = PsoCost {
            times: &data.times,
            flux: &data.flux,
            flux_err: &data.flux_err,
            is_upper: &data.is_upper,
            upper_flux: &data.upper_flux,
            model,
        };
        let problem = FixedT0PsoCost { inner, t0, t0_idx };
        let solver = ParticleSwarm::new((lower, upper), 40);
        match Executor::new(problem, solver)
            .configure(|state| state.max_iters(60))
            .run()
        {
            Ok(res) => {
                let best = res.state().get_best_param().unwrap();
                let cost = res.state().get_cost();
                let mut full = Vec::with_capacity(best.position.len() + 1);
                for (i, &val) in best.position.iter().enumerate() {
                    if i == t0_idx {
                        full.push(t0);
                    }
                    full.push(val);
                }
                if t0_idx >= best.position.len() {
                    full.push(t0);
                }
                (full, cost)
            }
            Err(_) => (vec![], f64::INFINITY),
        }
    };

    let mut best_params = pilot_params.to_vec();
    let mut best_cost = pilot_cost;
    let mut best_t0 = pilot_t0;

    for &t0 in &linspace(t0_min, t0_max, 10) {
        let (params, cost) = run_at_t0(t0);
        if cost < best_cost && !params.is_empty() {
            best_cost = cost;
            best_params = params;
            best_t0 = t0;
        }
    }

    let fine_min = (best_t0 - 0.5).max(t0_min);
    let fine_max = (best_t0 + 0.5).min(t0_max);
    for &t0 in &linspace(fine_min, fine_max, 5) {
        let (params, cost) = run_at_t0(t0);
        if cost < best_cost && !params.is_empty() {
            best_cost = cost;
            best_params = params;
        }
    }

    (best_params, best_cost)
}

// ---------------------------------------------------------------------------
// PSO model selection
// ---------------------------------------------------------------------------

fn pso_model_select(data: &BandFitData) -> (SviModel, Vec<f64>, f64) {
    const EARLY_STOP: f64 = -3.0;

    let models: &[SviModel] = &[
        SviModel::Bazin,
        SviModel::Arnett,
        SviModel::Tde,
        SviModel::Afterglow,
        SviModel::Villar,
        SviModel::Magnetar,
        SviModel::ShockCooling,
        SviModel::MetzgerKN,
    ];

    let mut best_model = SviModel::Bazin;
    let mut best_params = vec![];
    let mut best_chi2 = f64::INFINITY;

    for &model in models {
        let (lower, upper) = pso_bounds(model);
        let problem = PsoCost {
            times: &data.times,
            flux: &data.flux,
            flux_err: &data.flux_err,
            is_upper: &data.is_upper,
            upper_flux: &data.upper_flux,
            model,
        };
        let solver = ParticleSwarm::new((lower, upper), 40);
        let res = Executor::new(problem, solver)
            .configure(|state| state.max_iters(50))
            .run();
        match res {
            Ok(res) => {
                let chi2 = res.state().get_cost();
                if chi2 < best_chi2 {
                    best_chi2 = chi2;
                    best_model = model;
                    best_params = res.state().get_best_param().unwrap().position.clone();
                }
            }
            Err(_) => {}
        }
        if best_chi2 < EARLY_STOP {
            break;
        }
    }

    (best_model, best_params, best_chi2)
}

// ---------------------------------------------------------------------------
// Priors and initialization
// ---------------------------------------------------------------------------

fn prior_params(model: SviModel) -> Vec<(f64, f64)> {
    match model {
        SviModel::Bazin => vec![(0.0, 2.0); 6],
        SviModel::Villar => vec![(0.0, 2.0); 7],
        SviModel::MetzgerKN => {
            vec![
                (-1.75, 1.25),
                (-1.25, 0.75),
                (0.5, 1.5),
                (-0.5, 1.5),
                (0.0, 2.0),
            ]
        }
        SviModel::Tde => {
            let mut p = vec![(0.0, 2.0); 7];
            p[5] = (1.67, 1.0);
            p
        }
        SviModel::Arnett => {
            let mut p = vec![(0.0, 2.0); 5];
            p[2] = (2.3, 1.0);
            p
        }
        SviModel::Magnetar => {
            let mut p = vec![(0.0, 2.0); 5];
            p[2] = (3.0, 1.5);
            p[3] = (2.3, 1.0);
            p
        }
        SviModel::ShockCooling => {
            let mut p = vec![(0.0, 2.0); 5];
            p[2] = (0.5, 1.0);
            p[3] = (1.0, 2.0);
            p
        }
        SviModel::Afterglow => {
            let mut p = vec![(0.0, 2.0); 6];
            p[2] = (1.0, 2.0);
            p[3] = (0.8, 1.0);
            p[4] = (2.0, 1.0);
            p
        }
    }
}

fn init_variational_means(model: SviModel, data: &BandFitData) -> Vec<f64> {
    let mut peak_val = f64::NEG_INFINITY;
    let mut peak_idx = 0;
    for (i, &f) in data.flux.iter().enumerate() {
        if f > peak_val {
            peak_val = f;
            peak_idx = i;
        }
    }
    let t_peak = data.times[peak_idx];

    match model {
        SviModel::Bazin => {
            vec![
                peak_val.max(0.01).ln(),
                0.0,
                t_peak,
                2.0_f64.ln(),
                20.0_f64.ln(),
                -3.0,
            ]
        }
        SviModel::Villar => {
            vec![
                peak_val.max(0.01).ln(),
                0.01,
                10.0_f64.ln(),
                t_peak,
                2.0_f64.ln(),
                20.0_f64.ln(),
                (-3.0_f64).ln().max(-5.0),
            ]
        }
        SviModel::MetzgerKN => {
            vec![-2.0, -0.7, 0.5, t_peak - 2.0, -3.0]
        }
        SviModel::Tde => {
            vec![
                peak_val.max(0.01).ln(),
                0.0,
                t_peak - 10.0,
                2.0_f64.ln(),
                20.0_f64.ln(),
                1.67,
                -3.0,
            ]
        }
        SviModel::Arnett => {
            vec![
                peak_val.max(0.01).ln(),
                t_peak - 15.0,
                10.0_f64.ln(),
                0.0,
                -3.0,
            ]
        }
        SviModel::Magnetar => {
            vec![
                peak_val.max(0.01).ln(),
                t_peak - 10.0,
                20.0_f64.ln(),
                10.0_f64.ln(),
                -3.0,
            ]
        }
        SviModel::ShockCooling => {
            vec![
                peak_val.max(0.01).ln(),
                t_peak - 2.0,
                0.5,
                5.0_f64.ln(),
                -3.0,
            ]
        }
        SviModel::Afterglow => {
            vec![
                peak_val.max(0.01).ln(),
                t_peak - 5.0,
                10.0_f64.ln(),
                0.8,
                2.2,
                -3.0,
            ]
        }
    }
}

// ---------------------------------------------------------------------------
// SVI fitting
// ---------------------------------------------------------------------------

struct SviFitResult {
    model: SviModel,
    mu: Vec<f64>,
    log_sigma: Vec<f64>,
    elbo: f64,
}

fn svi_fit(
    model: SviModel,
    data: &BandFitData,
    n_steps: usize,
    n_samples: usize,
    lr: f64,
    pso_init: Option<&[f64]>,
) -> SviFitResult {
    let n_params = model.n_params();
    let n_variational = 2 * n_params;
    let n_obs = data.times.len();

    let mut var_params = vec![0.0; n_variational];
    let init_mu = if let Some(pso_params) = pso_init {
        pso_params.to_vec()
    } else {
        init_variational_means(model, data)
    };
    for i in 0..n_params {
        var_params[i] = init_mu[i];
        var_params[n_params + i] = -1.0;
    }

    let mut adam = ManualAdam::new(n_variational, lr);
    let mut rng = SmallRng::seed_from_u64(42);
    let obs_var: Vec<f64> = data.flux_err.iter().map(|e| e * e + 1e-10).collect();
    let se_idx = model.sigma_extra_idx();
    let priors = prior_params(model);

    let mut eps = vec![0.0; n_params];
    let mut theta = vec![0.0; n_params];
    let mut sigma = vec![0.0; n_params];
    let mut preds = vec![0.0; n_obs];
    let mut grad_buf = vec![0.0; n_params];
    let mut kn_grads_flat = if model.is_sequential() {
        vec![0.0; n_obs * n_params]
    } else {
        Vec::new()
    };
    let mut grad_mu = vec![0.0; n_params];
    let mut grad_log_sigma = vec![0.0; n_params];
    let mut dll_dtheta = vec![0.0; n_params];
    let mut neg_elbo_grad = vec![0.0; n_variational];

    let mut final_elbo = f64::NEG_INFINITY;

    for _step in 0..n_steps {
        let mu = &var_params[..n_params];
        let log_sigma = &var_params[n_params..];
        for j in 0..n_params {
            sigma[j] = log_sigma[j].exp();
        }

        for j in 0..n_params {
            grad_mu[j] = 0.0;
            grad_log_sigma[j] = 0.0;
        }
        let mut elbo_sum = 0.0;

        for _ in 0..n_samples {
            for j in 0..n_params {
                let u1: f64 = rng.random::<f64>().max(1e-10);
                let u2: f64 = rng.random::<f64>();
                eps[j] = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                theta[j] = mu[j] + sigma[j] * eps[j];
            }

            let sigma_extra = theta[se_idx].exp();
            let sigma_extra_sq = sigma_extra * sigma_extra;

            if model.is_sequential() {
                let batch = metzger_kn_eval_batch(&theta, &data.times);
                preds.copy_from_slice(&batch);
                metzger_kn_grad_batch_into(&theta, &data.times, &mut kn_grads_flat);
            } else {
                for (i, &t) in data.times.iter().enumerate() {
                    preds[i] = eval_model(model, &theta, t);
                }
            }

            if model == SviModel::MetzgerKN {
                let max_pred = preds
                    .iter()
                    .zip(data.is_upper.iter())
                    .filter(|(_, is_up)| !**is_up)
                    .map(|(p, _)| *p)
                    .fold(f64::NEG_INFINITY, f64::max);
                if max_pred > 1e-10 && max_pred.is_finite() {
                    let raw_scale = 1.0 / max_pred;
                    let scale = raw_scale.clamp(0.1, 10.0);
                    if (raw_scale - scale).abs() / raw_scale > 0.5 {
                        continue;
                    }
                    for pred in preds.iter_mut() {
                        *pred *= scale;
                    }
                    for g in kn_grads_flat.iter_mut() {
                        *g *= scale;
                    }
                }
            }

            let mut log_lik = 0.0;
            for j in 0..n_params {
                dll_dtheta[j] = 0.0;
            }

            for i in 0..n_obs {
                let pred = preds[i];
                if !pred.is_finite() {
                    continue;
                }
                let total_var = obs_var[i] + sigma_extra_sq;
                let sigma_total = total_var.sqrt();

                if model.is_sequential() {
                    let base_off = i * n_params;
                    grad_buf.copy_from_slice(&kn_grads_flat[base_off..base_off + n_params]);
                } else {
                    eval_model_grad(model, &theta, data.times[i], &mut grad_buf);
                }

                if data.is_upper[i] {
                    let z = (data.upper_flux[i] - pred) / sigma_total;
                    log_lik += log_normal_cdf(z);

                    let phi_z = (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt();
                    let cdf_z =
                        (0.5 * (1.0 + erf_approx(z * std::f64::consts::FRAC_1_SQRT_2))).max(1e-300);
                    let dll_dpred = -phi_z / (cdf_z * sigma_total);

                    for j in 0..n_params {
                        if j != se_idx && grad_buf[j].is_finite() {
                            dll_dtheta[j] += dll_dpred * grad_buf[j];
                        }
                    }

                    let dz_dlse =
                        -(data.upper_flux[i] - pred) * sigma_extra_sq / (sigma_total * total_var);
                    dll_dtheta[se_idx] += (phi_z / cdf_z) * dz_dlse;
                } else {
                    let residual = data.flux[i] - pred;
                    let inv_total = 1.0 / total_var;
                    let r2 = residual * residual;
                    log_lik +=
                        -0.5 * (r2 * inv_total + (2.0 * std::f64::consts::PI * total_var).ln());

                    for j in 0..n_params {
                        if j != se_idx && grad_buf[j].is_finite() {
                            dll_dtheta[j] += residual * inv_total * grad_buf[j];
                        }
                    }

                    dll_dtheta[se_idx] += (r2 * inv_total * inv_total - inv_total) * sigma_extra_sq;
                }
            }

            let mut log_prior = 0.0;
            for j in 0..n_params {
                let (center, width) = priors[j];
                let var = width * width;
                log_prior += -0.5 * (theta[j] - center).powi(2) / var;
                let dlp = -(theta[j] - center) / var;
                let df_dtheta = dll_dtheta[j] + dlp;
                grad_mu[j] += df_dtheta;
                grad_log_sigma[j] += df_dtheta * sigma[j] * eps[j];
            }

            elbo_sum += log_lik + log_prior;
        }

        let ns = n_samples as f64;
        for j in 0..n_params {
            grad_mu[j] /= ns;
            grad_log_sigma[j] /= ns;
        }
        elbo_sum /= ns;

        let log_sigma = &var_params[n_params..];
        let entropy: f64 = log_sigma.iter().sum::<f64>()
            + 0.5 * n_params as f64 * (2.0 * std::f64::consts::PI * std::f64::consts::E).ln();
        final_elbo = elbo_sum + entropy;

        for j in 0..n_params {
            neg_elbo_grad[j] = -grad_mu[j];
            neg_elbo_grad[n_params + j] = -(grad_log_sigma[j] + 1.0);
        }

        adam.step(&mut var_params, &neg_elbo_grad);

        for i in 0..n_params {
            var_params[n_params + i] = var_params[n_params + i].clamp(-6.0, 2.0);
        }
    }

    let mu = var_params[..n_params].to_vec();
    let log_inflation = SIGMA_INFLATION_FACTOR.ln();
    let log_sigma: Vec<f64> = var_params[n_params..]
        .iter()
        .map(|ls| ls + log_inflation)
        .collect();

    SviFitResult {
        model,
        mu,
        log_sigma,
        elbo: final_elbo,
    }
}

// ---------------------------------------------------------------------------
// Post-SVI t0 profile refinement
// ---------------------------------------------------------------------------

fn profile_t0_refine(result: &mut SviFitResult, data: &BandFitData) {
    let model = result.model;
    let t0_idx = model.t0_idx();
    let se_idx = model.sigma_extra_idx();

    let mu_t0 = result.mu[t0_idx];

    let t_first = data.times.iter().cloned().fold(f64::INFINITY, f64::min);
    let mut peak_idx = 0;
    let mut peak_val = f64::NEG_INFINITY;
    for (i, &f) in data.flux.iter().enumerate() {
        if f > peak_val {
            peak_val = f;
            peak_idx = i;
        }
    }
    let t_peak = data.times[peak_idx];
    let t0_lo = t_first - 30.0;
    let t0_hi = t_peak + 5.0;
    if t0_lo >= t0_hi {
        return;
    }

    let n_grid: usize = 50;
    let obs_var: Vec<f64> = data.flux_err.iter().map(|e| e * e + 1e-10).collect();
    let sigma_extra = result.mu[se_idx].exp();
    let sigma_extra_sq = sigma_extra * sigma_extra;

    let n_obs = data.times.len();
    let mut params = result.mu.clone();
    let mut best_t0 = mu_t0;
    let mut best_ll = f64::NEG_INFINITY;

    let log_a_idx = 0;
    let has_baseline = matches!(model, SviModel::Bazin | SviModel::Tde);

    for gi in 0..n_grid {
        let t0 = t0_lo + (t0_hi - t0_lo) * gi as f64 / (n_grid - 1).max(1) as f64;

        for j in 0..params.len() {
            params[j] = result.mu[j];
        }
        params[t0_idx] = t0;

        // Analytically re-fit amplitude
        {
            let saved_log_a = params[log_a_idx];
            params[log_a_idx] = 0.0;
            let saved_b = if has_baseline {
                let v = params[1];
                params[1] = 0.0;
                v
            } else {
                0.0
            };

            let shapes = eval_model_batch(model, &params, &data.times);

            if has_baseline {
                let mut sw = 0.0;
                let mut sy = 0.0;
                let mut sf = 0.0;
                let mut syf = 0.0;
                let mut sff = 0.0;
                for i in 0..n_obs {
                    if data.is_upper[i] || !shapes[i].is_finite() {
                        continue;
                    }
                    let w = 1.0 / (obs_var[i] + sigma_extra_sq);
                    sw += w;
                    sy += w * data.flux[i];
                    sf += w * shapes[i];
                    syf += w * data.flux[i] * shapes[i];
                    sff += w * shapes[i] * shapes[i];
                }
                let det = sw * sff - sf * sf;
                if det.abs() > 1e-20 {
                    let a_opt = (sw * syf - sf * sy) / det;
                    let b_opt = (sff * sy - sf * syf) / det;
                    if a_opt > 1e-10 {
                        params[log_a_idx] = a_opt.ln();
                        params[1] = b_opt;
                    } else {
                        params[log_a_idx] = saved_log_a;
                        params[1] = saved_b;
                    }
                } else {
                    params[log_a_idx] = saved_log_a;
                    params[1] = saved_b;
                }
            } else {
                let mut num = 0.0;
                let mut den = 0.0;
                for i in 0..n_obs {
                    if data.is_upper[i] || !shapes[i].is_finite() {
                        continue;
                    }
                    let w = 1.0 / (obs_var[i] + sigma_extra_sq);
                    num += w * data.flux[i] * shapes[i];
                    den += w * shapes[i] * shapes[i];
                }
                if den > 1e-20 && num / den > 1e-10 {
                    params[log_a_idx] = (num / den).ln();
                } else {
                    params[log_a_idx] = saved_log_a;
                }
            }
        }

        let mut preds = eval_model_batch(model, &params, &data.times);

        if model == SviModel::MetzgerKN {
            let max_pred = preds
                .iter()
                .zip(data.is_upper.iter())
                .filter(|(_, is_up)| !**is_up)
                .map(|(p, _)| *p)
                .fold(f64::NEG_INFINITY, f64::max);
            if max_pred > 1e-10 && max_pred.is_finite() {
                let scale = (1.0 / max_pred).clamp(0.1, 10.0);
                for pred in preds.iter_mut() {
                    *pred *= scale;
                }
            }
        }

        let se = params[se_idx].exp();
        let se_sq = se * se;
        let mut ll = 0.0;
        for i in 0..n_obs {
            let pred = preds[i];
            if !pred.is_finite() {
                continue;
            }
            let total_var = obs_var[i] + se_sq;
            if data.is_upper[i] {
                let z = (data.upper_flux[i] - pred) / total_var.sqrt();
                ll += log_normal_cdf(z);
            } else {
                let residual = data.flux[i] - pred;
                ll += -0.5
                    * (residual * residual / total_var
                        + (2.0 * std::f64::consts::PI * total_var).ln());
            }
        }

        if ll > best_ll {
            best_ll = ll;
            best_t0 = t0;
        }
    }

    // Profile width (simplified: use grid range as proxy for profile sigma)
    let profile_sigma = ((t0_hi - t0_lo) / 10.0).max(0.01);

    result.mu[t0_idx] = best_t0;
    result.log_sigma[t0_idx] = profile_sigma.ln();
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

struct BandFitData {
    times: Vec<f64>,
    flux: Vec<f64>,
    flux_err: Vec<f64>,
    is_upper: Vec<bool>,
    upper_flux: Vec<f64>,
    #[allow(dead_code)]
    noise_frac_median: f64,
    peak_flux_obs: f64,
}

fn median_f64(xs: &mut [f64]) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = xs.len() / 2;
    if xs.len() % 2 == 0 {
        Some((xs[mid - 1] + xs[mid]) / 2.0)
    } else {
        Some(xs[mid])
    }
}

fn flux_to_mag(flux: f64, zp: f64) -> f64 {
    -2.5 * flux.log10() + zp
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Result of parametric SVI fitting for a single band.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParametricBandResult {
    pub band: String,
    pub model: SviModelName,
    pub pso_params: Vec<f64>,
    pub pso_chi2: Option<f64>,
    pub svi_mu: Vec<f64>,
    pub svi_log_sigma: Vec<f64>,
    pub svi_elbo: Option<f64>,
    pub n_obs: usize,
    pub mag_chi2: Option<f64>,
}

/// Fit parametric lightcurve models to all bands.
///
/// `bands` maps band names to `BandData` containing flux values.
/// Uses PSO for model selection, then SVI for posterior inference.
pub fn fit_parametric(bands: &HashMap<String, BandData>) -> Vec<ParametricBandResult> {
    if bands.is_empty() {
        return Vec::new();
    }

    let snr_threshold = 3.0;
    let svi_lr = 0.01;
    let svi_n_steps = 1000;
    let svi_n_samples = 4;
    let zp = 23.9;

    let mut band_entries: Vec<(String, BandFitData, Vec<f64>)> = Vec::new();

    for (band_name, band_data) in bands {
        let fluxes = &band_data.values;
        let flux_errs = &band_data.errors;
        let times = &band_data.times;

        if fluxes.is_empty() {
            continue;
        }
        let peak_flux = fluxes.iter().cloned().fold(f64::MIN, f64::max);
        if peak_flux <= 0.0 {
            continue;
        }

        let normalized_flux: Vec<f64> = fluxes.iter().map(|f| f / peak_flux).collect();
        let normalized_err: Vec<f64> = flux_errs.iter().map(|e| e / peak_flux).collect();

        let is_upper: Vec<bool> = fluxes
            .iter()
            .zip(flux_errs.iter())
            .map(|(f, e)| *e > 0.0 && (*f / *e) < snr_threshold)
            .collect();
        let upper_flux: Vec<f64> = flux_errs
            .iter()
            .map(|e| snr_threshold * e / peak_flux)
            .collect();

        let mut frac_noises: Vec<f64> = normalized_flux
            .iter()
            .zip(normalized_err.iter())
            .filter_map(|(f, e)| if *f > 0.0 { Some(e / f) } else { None })
            .collect();
        let noise_frac_median = median_f64(&mut frac_noises).unwrap_or(0.0);

        let mags_obs: Vec<f64> = fluxes.iter().map(|f| flux_to_mag(*f, zp)).collect();

        let fit_data = BandFitData {
            times: times.clone(),
            flux: normalized_flux,
            flux_err: normalized_err,
            is_upper,
            upper_flux,
            noise_frac_median,
            peak_flux_obs: peak_flux,
        };

        band_entries.push((band_name.clone(), fit_data, mags_obs));
    }

    band_entries.sort_by(|a, b| b.1.times.len().cmp(&a.1.times.len()));

    let mut results: Vec<ParametricBandResult> = Vec::new();

    for (band_idx, (band_name, data, mags_obs)) in band_entries.iter().enumerate() {
        // Step 1: PSO model selection
        let (pso_model, pso_params, pso_chi2) = pso_model_select(data);

        if pso_params.is_empty() {
            continue;
        }

        // Step 1b: Profile t0 (reference band only)
        let (pso_params, pso_chi2) = if band_idx == 0 {
            profile_t0_search(data, pso_model, &pso_params, pso_chi2)
        } else {
            (pso_params, pso_chi2)
        };

        // Step 2: SVI refinement
        let mut svi_result = svi_fit(
            pso_model,
            data,
            svi_n_steps,
            svi_n_samples,
            svi_lr,
            Some(&pso_params),
        );

        // Step 3: Profile-likelihood refinement of t0
        profile_t0_refine(&mut svi_result, data);

        // Compute reduced chi² in magnitude space
        let svi_preds = eval_model_batch(pso_model, &svi_result.mu, &data.times);
        let mut mag_chi2_sum = 0.0;
        let mut mag_chi2_n = 0usize;
        for i in 0..data.times.len() {
            let pred_flux = svi_preds[i] * data.peak_flux_obs;
            if pred_flux > 0.0 && data.flux[i] > 0.0 {
                let mag_pred = flux_to_mag(pred_flux, zp);
                let mag_obs = mags_obs[i];
                // Use unnormalized flux for correct magnitude error
                let flux_obs = data.flux[i] * data.peak_flux_obs;
                let mag_err = 1.0857 * data.flux_err[i] / flux_obs;
                if mag_err > 0.0 {
                    let residual = mag_obs - mag_pred;
                    mag_chi2_sum += residual * residual / (mag_err * mag_err);
                    mag_chi2_n += 1;
                }
            }
        }
        let mag_chi2 = if mag_chi2_n > 0 {
            mag_chi2_sum / mag_chi2_n as f64
        } else {
            f64::NAN
        };

        results.push(ParametricBandResult {
            band: band_name.clone(),
            model: pso_model.to_name(),
            pso_params: pso_params.clone(),
            pso_chi2: finite_or_none(pso_chi2),
            svi_mu: svi_result.mu.clone(),
            svi_log_sigma: svi_result.log_sigma.clone(),
            svi_elbo: finite_or_none(svi_result.elbo),
            n_obs: data.times.len(),
            mag_chi2: finite_or_none(mag_chi2),
        });
    }

    results
}
