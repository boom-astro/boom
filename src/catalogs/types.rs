//! Record types: one per catalog, defining both how a source row is parsed and
//! what the stored document looks like.
//!
//! Field names and renames are load-bearing -- the crossmatch projections in
//! `config.yaml` are written against them -- so they mirror each catalog's
//! published column names rather than being normalized to a house style.

use super::ingest::HasCoordinates;
use serde::{Deserialize, Serialize};

/// Parse a whitespace-trimmed field, treating blank as absent.
fn optional<T: std::str::FromStr>(field: &str) -> Option<T> {
    let trimmed = field.trim();
    (!trimmed.is_empty())
        .then(|| trimmed.parse().ok())
        .flatten()
}

/// Same, for strings, where `parse` would accept the empty string.
fn optional_string(field: &str) -> Option<String> {
    let trimmed = field.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// A byte range of a fixed-width line, tolerating a line shorter than declared.
///
/// The published files pad to a fixed width, but a truncated final line should
/// produce a parse error naming the field rather than a panic.
fn column(line: &str, start: usize, end: usize) -> &str {
    if start >= line.len() {
        return "";
    }
    &line[start..end.min(line.len())]
}

// ---------------------------------------------------------------------------
// 2MASS -- ascii
// ---------------------------------------------------------------------------

/// One row of the 2MASS Point Source Catalog.
///
/// Format: <https://irsa.ipac.caltech.edu/2MASS/download/allsky/format_psc.html>
/// Pipe-delimited with 60 columns; the subset kept here is what the crossmatch
/// projections read.
#[derive(Debug, Serialize, Deserialize)]
pub struct TwoMass {
    #[serde(rename(serialize = "_id"))]
    pub designation: String,
    pub ra: f64,
    pub dec: f64,
    pub j_m: Option<f32>,
    pub j_cmsig: Option<f32>,
    pub j_msigcom: Option<f32>,
    pub j_snr: Option<f32>,
    pub h_m: Option<f32>,
    pub h_cmsig: Option<f32>,
    pub h_msigcom: Option<f32>,
    pub h_snr: Option<f32>,
    pub k_m: Option<f32>,
    pub k_cmsig: Option<f32>,
    pub k_msigcom: Option<f32>,
    pub k_snr: Option<f32>,
    pub ph_qual: Option<String>,
    pub rd_flg: Option<String>,
    pub bl_flg: Option<String>,
    pub cc_flg: Option<String>,
    pub ndet: Option<i32>,
}

/// Columns of the PSC record, by published position.
mod twomass_col {
    pub const RA: usize = 0;
    pub const DEC: usize = 1;
    pub const DESIGNATION: usize = 5;
    pub const J_M: usize = 6;
    pub const NDET: usize = 22;
    /// The published record has 60 fields; a shorter line is truncated, not a
    /// variant we should half-parse.
    pub const COUNT: usize = 60;
}

impl super::ascii::FromAsciiRow for TwoMass {
    fn from_line(line: &str) -> Result<Self, String> {
        let f: Vec<&str> = line.split('|').collect();
        if f.len() < twomass_col::COUNT {
            return Err(format!(
                "expected {} fields, found {}",
                twomass_col::COUNT,
                f.len()
            ));
        }
        // ra/dec are stored as f64 even though 2MASS publishes ~6 decimal
        // places, so the coordinates subdocument matches every other catalog.
        let ra = f[twomass_col::RA]
            .trim()
            .parse::<f64>()
            .map_err(|e| format!("ra {:?}: {}", f[twomass_col::RA], e))?;
        let dec = f[twomass_col::DEC]
            .trim()
            .parse::<f64>()
            .map_err(|e| format!("dec {:?}: {}", f[twomass_col::DEC], e))?;
        let designation = f[twomass_col::DESIGNATION].trim();
        if designation.is_empty() {
            return Err("empty designation".to_string());
        }
        let j = twomass_col::J_M;
        Ok(TwoMass {
            designation: designation.to_string(),
            ra,
            dec,
            j_m: optional(f[j]),
            j_cmsig: optional(f[j + 1]),
            j_msigcom: optional(f[j + 2]),
            j_snr: optional(f[j + 3]),
            h_m: optional(f[j + 4]),
            h_cmsig: optional(f[j + 5]),
            h_msigcom: optional(f[j + 6]),
            h_snr: optional(f[j + 7]),
            k_m: optional(f[j + 8]),
            k_cmsig: optional(f[j + 9]),
            k_msigcom: optional(f[j + 10]),
            k_snr: optional(f[j + 11]),
            ph_qual: optional_string(f[j + 12]),
            rd_flg: optional_string(f[j + 13]),
            bl_flg: optional_string(f[j + 14]),
            cc_flg: optional_string(f[j + 15]),
            ndet: optional(f[twomass_col::NDET]),
        })
    }
}

impl HasCoordinates for TwoMass {}

// ---------------------------------------------------------------------------
// NED-LVS -- parquet, converted from the published FITS table by boompy
// ---------------------------------------------------------------------------

/// One row of the NED Local Volume Sample.
///
/// Deliberately not `skip_serializing_none`: dropping absent fields entirely
/// makes consumers that project them error on a missing key, so every field is
/// present with an explicit null. Mongo keys mirror the published FITS column
/// names so a projection can be written against the column list.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Ned {
    #[serde(rename(serialize = "_id"))]
    pub objname: String,
    pub ra: f64,
    pub dec: f64,
    pub objtype: String,
    pub z: Option<f64>,
    pub z_unc: Option<f64>,
    pub z_tech: String,
    pub z_qual: bool,
    pub z_refcode: String,
    /// Redshift-independent for only ~1.2% of rows; gate on `DistMpc_method`
    /// before treating this as anything but a converted redshift.
    #[serde(rename(serialize = "DistMpc"))]
    pub dist_mpc: Option<f64>,
    #[serde(rename(serialize = "DistMpc_unc"))]
    pub dist_mpc_unc: Option<f64>,
    #[serde(rename(serialize = "DistMpc_method"))]
    pub dist_mpc_method: String,
    /// Angular diameter ellipse, added to NED-LVS in the 2026-04-24 release.
    #[serde(rename(serialize = "Diam"))]
    pub diam: Option<f64>,
    #[serde(rename(serialize = "Diam_ra"))]
    pub diam_ra: Option<f64>,
    #[serde(rename(serialize = "Diam_dec"))]
    pub diam_dec: Option<f64>,
    #[serde(rename(serialize = "Diam_ba"))]
    pub diam_ba: Option<f64>,
    #[serde(rename(serialize = "Diam_pa"))]
    pub diam_pa: Option<f64>,
    #[serde(rename(serialize = "Diam_survey"))]
    pub diam_survey: String,
    #[serde(rename(serialize = "Diam_filt"))]
    pub diam_filt: String,
    #[serde(rename(serialize = "Diam_refcode"))]
    pub diam_refcode: String,
    #[serde(rename(serialize = "Diam_qual"))]
    pub diam_qual: bool,
    pub ebv: Option<f64>,
    #[serde(rename(serialize = "m_Ks"))]
    pub m_ks: Option<f64>,
    #[serde(rename(serialize = "m_Ks_unc"))]
    pub m_ks_unc: Option<f64>,
    /// 2MASS photometry provenance, e.g. "2MASX"; empty when no 2MASS match.
    #[serde(rename(serialize = "tMASSphot"))]
    pub tmass_phot: String,
    #[serde(rename(serialize = "Mstar"))]
    pub m_star: Option<f64>,
    #[serde(rename(serialize = "Mstar_unc"))]
    pub m_star_unc: Option<f64>,
    #[serde(rename(serialize = "MLratio"))]
    pub ml_ratio: Option<f64>,
}

impl super::arrow::FromRecordBatch for Ned {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{bool_column, f64_column, string_column};

        // Column names are the published NED-LVS FITS ones, preserved through
        // the conversion in boompy so the projection here reads like the
        // catalog's own documentation.
        let objname = string_column(batch, "objname")?;
        let ra = f64_column(batch, "ra")?;
        let dec = f64_column(batch, "dec")?;
        let objtype = string_column(batch, "objtype")?;
        let z = f64_column(batch, "z")?;
        let z_unc = f64_column(batch, "z_unc")?;
        let z_tech = string_column(batch, "z_tech")?;
        let z_qual = bool_column(batch, "z_qual")?;
        let z_refcode = string_column(batch, "z_refcode")?;
        let dist_mpc = f64_column(batch, "DistMpc")?;
        let dist_mpc_unc = f64_column(batch, "DistMpc_unc")?;
        let dist_mpc_method = string_column(batch, "DistMpc_method")?;
        let diam = f64_column(batch, "Diam")?;
        let diam_ra = f64_column(batch, "Diam_ra")?;
        let diam_dec = f64_column(batch, "Diam_dec")?;
        let diam_ba = f64_column(batch, "Diam_ba")?;
        let diam_pa = f64_column(batch, "Diam_pa")?;
        let diam_survey = string_column(batch, "Diam_survey")?;
        let diam_filt = string_column(batch, "Diam_filt")?;
        let diam_refcode = string_column(batch, "Diam_refcode")?;
        let diam_qual = bool_column(batch, "Diam_qual")?;
        let ebv = f64_column(batch, "ebv")?;
        let m_ks = f64_column(batch, "m_Ks")?;
        let m_ks_unc = f64_column(batch, "m_Ks_unc")?;
        let tmass_phot = string_column(batch, "tMASSphot")?;
        let m_star = f64_column(batch, "Mstar")?;
        let m_star_unc = f64_column(batch, "Mstar_unc")?;
        let ml_ratio = f64_column(batch, "MLratio")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            // A galaxy with no name or no position cannot be crossmatched and
            // would be rejected by the 2dsphere index; skip rather than invent.
            let (Some(objname), Some(ra), Some(dec)) = (objname[i].clone(), ra[i], dec[i]) else {
                continue;
            };
            rows.push(Ned {
                objname,
                ra,
                dec,
                // The string fields are always present in the document, as
                // empty strings rather than nulls, because consumers project
                // them unconditionally.
                objtype: objtype[i].clone().unwrap_or_default(),
                z: z[i],
                z_unc: z_unc[i],
                z_tech: z_tech[i].clone().unwrap_or_default(),
                z_qual: z_qual[i].unwrap_or(false),
                z_refcode: z_refcode[i].clone().unwrap_or_default(),
                dist_mpc: dist_mpc[i],
                dist_mpc_unc: dist_mpc_unc[i],
                dist_mpc_method: dist_mpc_method[i].clone().unwrap_or_default(),
                diam: diam[i],
                diam_ra: diam_ra[i],
                diam_dec: diam_dec[i],
                diam_ba: diam_ba[i],
                diam_pa: diam_pa[i],
                diam_survey: diam_survey[i].clone().unwrap_or_default(),
                diam_filt: diam_filt[i].clone().unwrap_or_default(),
                diam_refcode: diam_refcode[i].clone().unwrap_or_default(),
                diam_qual: diam_qual[i].unwrap_or(false),
                ebv: ebv[i],
                m_ks: m_ks[i],
                m_ks_unc: m_ks_unc[i],
                tmass_phot: tmass_phot[i].clone().unwrap_or_default(),
                m_star: m_star[i],
                m_star_unc: m_star_unc[i],
                ml_ratio: ml_ratio[i],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for Ned {}

// ---------------------------------------------------------------------------
// LSPSC -- gzipped JSONL, exported from BOOM's own copy
// ---------------------------------------------------------------------------

/// One Legacy Survey point-source score.
///
/// The field names are the ones crossmatch config already projects (`score`,
/// `mag_white`), not the ones the upstream service returns (`xgboost`,
/// `white_mag`). The collection was loaded under these names before BOOM had a
/// definition for it, and renaming them now would break every deployment's
/// crossmatch projection and the filters reading them.
#[derive(Debug, Serialize, Deserialize)]
pub struct Lspsc {
    /// The Legacy Survey id, which the upstream service calls `lsid`.
    #[serde(rename = "_id")]
    pub id: i64,
    pub ra: f64,
    pub dec: f64,
    /// Probability the source is unresolved. Optional because an export of a
    /// collection loaded before the column existed would leave it empty rather
    /// than fail.
    pub score: Option<f64>,
    pub mag_white: Option<f64>,
}

impl HasCoordinates for Lspsc {}

// ---------------------------------------------------------------------------
// AllWISE -- parquet
// ---------------------------------------------------------------------------

/// One row of the AllWISE source catalog, as published in the LSDB HATS mirror
/// at <https://data.lsdb.io/hats/wise/allwise>.
///
/// Absent photometry is genuinely absent here (parquet has nulls), so unlike
/// NED the optional fields are omitted from the document rather than nulled.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct AllWise {
    #[serde(rename(serialize = "_id"))]
    pub source_id: String,
    pub ra: f64,
    pub dec: f64,
    pub sigra: f64,
    pub sigdec: f64,
    pub w1mpro: Option<f64>,
    pub w2mpro: Option<f64>,
    pub w3mpro: Option<f64>,
    pub w4mpro: Option<f64>,
    pub w1sigmpro: Option<f64>,
    pub w2sigmpro: Option<f64>,
    pub w3sigmpro: Option<f64>,
    pub w4sigmpro: Option<f64>,
    pub w1rchi2: Option<f64>,
    pub w2rchi2: Option<f64>,
    pub pmra: Option<f64>,
    pub pmdec: Option<f64>,
    pub sigpmra: Option<f64>,
    pub sigpmdec: Option<f64>,
}

impl super::arrow::FromRecordBatch for AllWise {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, string_column};

        // boompy projects these out of the HATS catalog; naming them again here
        // is how the reader says what it needs, and a column the projection
        // stopped emitting fails loudly with the column name.
        let source_id = string_column(batch, "source_id")?;
        let ra = f64_column(batch, "ra")?;
        let dec = f64_column(batch, "dec")?;
        let sigra = f64_column(batch, "sigra")?;
        let sigdec = f64_column(batch, "sigdec")?;
        let w1mpro = f64_column(batch, "w1mpro")?;
        let w2mpro = f64_column(batch, "w2mpro")?;
        let w3mpro = f64_column(batch, "w3mpro")?;
        let w4mpro = f64_column(batch, "w4mpro")?;
        let w1sigmpro = f64_column(batch, "w1sigmpro")?;
        let w2sigmpro = f64_column(batch, "w2sigmpro")?;
        let w3sigmpro = f64_column(batch, "w3sigmpro")?;
        let w4sigmpro = f64_column(batch, "w4sigmpro")?;
        let w1rchi2 = f64_column(batch, "w1rchi2")?;
        let w2rchi2 = f64_column(batch, "w2rchi2")?;
        let pmra = f64_column(batch, "pmra")?;
        let pmdec = f64_column(batch, "pmdec")?;
        let sigpmra = f64_column(batch, "sigpmra")?;
        let sigpmdec = f64_column(batch, "sigpmdec")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            // A source with no id or no position cannot be crossmatched and
            // would fail the 2dsphere index; skip rather than fabricate.
            let (Some(source_id), Some(ra), Some(dec), Some(sigra), Some(sigdec)) =
                (source_id[i].clone(), ra[i], dec[i], sigra[i], sigdec[i])
            else {
                continue;
            };
            rows.push(AllWise {
                source_id,
                ra,
                dec,
                sigra,
                sigdec,
                w1mpro: w1mpro[i],
                w2mpro: w2mpro[i],
                w3mpro: w3mpro[i],
                w4mpro: w4mpro[i],
                w1sigmpro: w1sigmpro[i],
                w2sigmpro: w2sigmpro[i],
                w3sigmpro: w3sigmpro[i],
                w4sigmpro: w4sigmpro[i],
                w1rchi2: w1rchi2[i],
                w2rchi2: w2rchi2[i],
                pmra: pmra[i],
                pmdec: pmdec[i],
                sigpmra: sigpmra[i],
                sigpmdec: sigpmdec[i],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for AllWise {}

// ---------------------------------------------------------------------------
// Milliquas -- parquet, converted from the published FITS table by boompy
// ---------------------------------------------------------------------------

/// One row of the Million Quasars catalog.
///
/// <https://quasars.org/milliquas.htm>
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct Milliquas {
    #[serde(rename(serialize = "_id"))]
    pub name: String,
    pub ra: f64,
    pub dec: f64,
    pub objtype: String,
    pub rmag: Option<f64>,
    pub bmag: Option<f64>,
    pub comment: Option<String>,
    pub rclass: Option<String>,
    pub bclass: Option<String>,
    pub z: Option<f64>,
    pub xname: Option<String>,
    pub rname: Option<String>,
    pub lobe1: Option<String>,
    pub lobe2: Option<String>,
}

impl super::arrow::FromRecordBatch for Milliquas {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, string_column};

        let name = string_column(batch, "NAME")?;
        let ra = f64_column(batch, "RA")?;
        let dec = f64_column(batch, "DEC")?;
        let objtype = string_column(batch, "TYPE")?;
        let rmag = f64_column(batch, "RMAG")?;
        let bmag = f64_column(batch, "BMAG")?;
        let comment = string_column(batch, "COMMENT")?;
        // `R` and `B` in the published table; the struct names them for what
        // they are rather than repeating the file's one-letter columns.
        let rclass = string_column(batch, "R")?;
        let bclass = string_column(batch, "B")?;
        let z = f64_column(batch, "Z")?;
        let xname = string_column(batch, "XNAME")?;
        let rname = string_column(batch, "RNAME")?;
        let lobe1 = string_column(batch, "LOBE1")?;
        let lobe2 = string_column(batch, "LOBE2")?;

        // Empty strings are how the FITS table spells "absent" for the optional
        // text fields; storing "" would make a projection look populated.
        let blank_to_none = |v: &Option<String>| v.clone().filter(|s| !s.is_empty());

        let mut rows = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let (Some(name), Some(ra), Some(dec)) = (name[i].clone(), ra[i], dec[i]) else {
                continue;
            };
            rows.push(Milliquas {
                name,
                ra,
                dec,
                objtype: objtype[i].clone().unwrap_or_default(),
                rmag: rmag[i],
                bmag: bmag[i],
                comment: blank_to_none(&comment[i]),
                rclass: blank_to_none(&rclass[i]),
                bclass: blank_to_none(&bclass[i]),
                z: z[i],
                xname: blank_to_none(&xname[i]),
                rname: blank_to_none(&rname[i]),
                lobe1: blank_to_none(&lobe1[i]),
                lobe2: blank_to_none(&lobe2[i]),
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for Milliquas {}

// ---------------------------------------------------------------------------
// DESI DR1 -- parquet, converted from the iron zcatalog by boompy
// ---------------------------------------------------------------------------

/// One spectroscopic redshift from the DESI DR1 `zall-tilecumulative-iron`
/// catalog.
///
/// boompy applies the row filters before conversion: only `ZCAT_PRIMARY` rows
/// (one best spectrum per target) and only positive `TARGETID`s (negative ids
/// are sky fibers -- not unique, no real source, and often with non-finite
/// coordinates the 2dsphere index would reject).
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct DesiDr1 {
    #[serde(rename(serialize = "_id"))]
    pub targetid: i64,
    pub ra: f64,
    pub dec: f64,
    pub survey: String,
    pub program: String,
    pub z: f64,
    /// Absent rather than `NaN` when the pipeline did not report one.
    ///
    /// MongoDB sorts `NaN` below every number, so storing it would make a
    /// quality cut like `{zerr: {$lt: 0.001}}` match rows whose redshift error
    /// was never measured -- the same trap `arrow::f64_column` exists to avoid
    /// for every other column. Omitted from the document when absent, which is
    /// what the struct's `skip_serializing_none` does for `subtype` too.
    pub zerr: Option<f64>,
    pub zwarn: i64,
    pub chi2: Option<f64>,
    pub deltachi2: Option<f64>,
    pub spectype: String,
    pub subtype: Option<String>,
    pub zcat_nspec: i64,
}

impl super::arrow::FromRecordBatch for DesiDr1 {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column, string_column};

        let targetid = i64_column(batch, "TARGETID")?;
        let ra = f64_column(batch, "TARGET_RA")?;
        let dec = f64_column(batch, "TARGET_DEC")?;
        let survey = string_column(batch, "SURVEY")?;
        let program = string_column(batch, "PROGRAM")?;
        let z = f64_column(batch, "Z")?;
        let zerr = f64_column(batch, "ZERR")?;
        let zwarn = i64_column(batch, "ZWARN")?;
        let chi2 = f64_column(batch, "CHI2")?;
        let deltachi2 = f64_column(batch, "DELTACHI2")?;
        let spectype = string_column(batch, "SPECTYPE")?;
        let subtype = string_column(batch, "SUBTYPE")?;
        let zcat_nspec = i64_column(batch, "ZCAT_NSPEC")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let (Some(targetid), Some(ra), Some(dec), Some(z)) = (targetid[i], ra[i], dec[i], z[i])
            else {
                continue;
            };
            rows.push(DesiDr1 {
                targetid,
                ra,
                dec,
                survey: survey[i].clone().unwrap_or_default(),
                program: program[i].clone().unwrap_or_default(),
                z,
                zerr: zerr[i],
                zwarn: zwarn[i].unwrap_or_default(),
                chi2: chi2[i],
                deltachi2: deltachi2[i],
                spectype: spectype[i].clone().unwrap_or_default(),
                subtype: subtype[i].clone().filter(|s| !s.is_empty()),
                zcat_nspec: zcat_nspec[i].unwrap_or_default(),
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for DesiDr1 {}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct DesiDr1Dwarf {
    #[serde(rename(serialize = "_id"))]
    pub targetid: i64,
    pub ra: f64,
    pub dec: f64,
    pub desiname: String,
    pub survey: String,
    pub program: String,
    pub z: f64,
    pub z_cmb: Option<f64>,
    pub zwarn: i64,
    pub deltachi2: Option<f64>,
    pub lumi_dist_mpc: Option<f64>,
    pub dist_source: String,
    pub log_mstar_m24: Option<f64>,
    pub log_mstar_m24_err: Option<f64>,
    pub mag_g: Option<f64>,
    pub mag_r: Option<f64>,
    pub mag_z: Option<f64>,
    pub r50_r: Option<f64>,
    pub dwarf_maskbit: i64,
    pub mstar_maskbit: i64,
    pub in_sga_2020: bool,
}

impl super::arrow::FromRecordBatch for DesiDr1Dwarf {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{bool_column, f64_column, i64_column, string_column};

        let targetid = i64_column(batch, "TARGETID")?;
        let ra = f64_column(batch, "RA")?;
        let dec = f64_column(batch, "DEC")?;
        let desiname = string_column(batch, "DESINAME")?;
        let survey = string_column(batch, "SURVEY")?;
        let program = string_column(batch, "PROGRAM")?;
        let z = f64_column(batch, "Z")?;
        let z_cmb = f64_column(batch, "Z_CMB")?;
        let zwarn = i64_column(batch, "ZWARN")?;
        let deltachi2 = f64_column(batch, "DELTACHI2")?;
        let lumi_dist_mpc = f64_column(batch, "LUMI_DIST_MPC")?;
        let dist_source = string_column(batch, "DIST_SOURCE")?;
        let log_mstar_m24 = f64_column(batch, "LOG_MSTAR_M24")?;
        let log_mstar_m24_err = f64_column(batch, "LOG_MSTAR_M24_ERR")?;
        let mag_g = f64_column(batch, "MAG_G")?;
        let mag_r = f64_column(batch, "MAG_R")?;
        let mag_z = f64_column(batch, "MAG_Z")?;
        let r50_r = f64_column(batch, "R50_R")?;
        let dwarf_maskbit = i64_column(batch, "DWARF_MASKBIT")?;
        let mstar_maskbit = i64_column(batch, "MSTAR_MASKBIT")?;
        let in_sga_2020 = bool_column(batch, "IN_SGA_2020")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let (Some(targetid), Some(ra), Some(dec), Some(z)) = (targetid[i], ra[i], dec[i], z[i])
            else {
                continue;
            };
            rows.push(DesiDr1Dwarf {
                targetid,
                ra,
                dec,
                desiname: desiname[i].clone().unwrap_or_default(),
                survey: survey[i].clone().unwrap_or_default(),
                program: program[i].clone().unwrap_or_default(),
                z,
                z_cmb: z_cmb[i],
                zwarn: zwarn[i].unwrap_or_default(),
                deltachi2: deltachi2[i],
                lumi_dist_mpc: lumi_dist_mpc[i],
                dist_source: dist_source[i].clone().unwrap_or_default(),
                log_mstar_m24: log_mstar_m24[i],
                log_mstar_m24_err: log_mstar_m24_err[i],
                mag_g: mag_g[i],
                mag_r: mag_r[i],
                mag_z: mag_z[i],
                r50_r: r50_r[i],
                dwarf_maskbit: dwarf_maskbit[i].unwrap_or_default(),
                mstar_maskbit: mstar_maskbit[i].unwrap_or_default(),
                in_sga_2020: in_sga_2020[i].unwrap_or_default(),
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for DesiDr1Dwarf {}

// ---------------------------------------------------------------------------
// CatWISE2020 -- parquet, converted from the published .tbl tables by boompy
// ---------------------------------------------------------------------------

/// One row of the CatWISE2020 catalog.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct CatWise2020 {
    #[serde(rename(serialize = "_id"))]
    pub source_id: String,
    pub source_name: String,
    pub ra: f64,
    pub dec: f64,
    pub sigra: f64,
    pub sigdec: f64,
    pub w1mpro: Option<f64>,
    pub w2mpro: Option<f64>,
    pub w1sigmpro: Option<f64>,
    pub w2sigmpro: Option<f64>,
    pub w1rchi2: Option<f64>,
    pub w2rchi2: Option<f64>,
    pub pmra: Option<f64>,
    pub pmdec: Option<f64>,
    pub sigpmra: Option<f64>,
    pub sigpmdec: Option<f64>,
    pub unwise_objid: Option<String>,
}

impl super::arrow::FromRecordBatch for CatWise2020 {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, string_column};

        let source_id = string_column(batch, "source_id")?;
        let source_name = string_column(batch, "source_name")?;
        let ra = f64_column(batch, "ra")?;
        let dec = f64_column(batch, "dec")?;
        let sigra = f64_column(batch, "sigra")?;
        let sigdec = f64_column(batch, "sigdec")?;
        let w1mpro = f64_column(batch, "w1mpro")?;
        let w2mpro = f64_column(batch, "w2mpro")?;
        let w1sigmpro = f64_column(batch, "w1sigmpro")?;
        let w2sigmpro = f64_column(batch, "w2sigmpro")?;
        let w1rchi2 = f64_column(batch, "w1rchi2")?;
        let w2rchi2 = f64_column(batch, "w2rchi2")?;
        let pmra = f64_column(batch, "pmra")?;
        let pmdec = f64_column(batch, "pmdec")?;
        let sigpmra = f64_column(batch, "sigpmra")?;
        let sigpmdec = f64_column(batch, "sigpmdec")?;
        let unwise_objid = string_column(batch, "unwise_objid")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let (Some(source_id), Some(ra), Some(dec)) = (source_id[i].clone(), ra[i], dec[i])
            else {
                continue;
            };
            rows.push(CatWise2020 {
                source_id,
                source_name: source_name[i].clone().unwrap_or_default(),
                ra,
                dec,
                sigra: sigra[i].unwrap_or_default(),
                sigdec: sigdec[i].unwrap_or_default(),
                w1mpro: w1mpro[i],
                w2mpro: w2mpro[i],
                w1sigmpro: w1sigmpro[i],
                w2sigmpro: w2sigmpro[i],
                w1rchi2: w1rchi2[i],
                w2rchi2: w2rchi2[i],
                pmra: pmra[i],
                pmdec: pmdec[i],
                sigpmra: sigpmra[i],
                sigpmdec: sigpmdec[i],
                unwise_objid: unwise_objid[i].clone().filter(|s| !s.is_empty()),
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for CatWise2020 {}

// ---------------------------------------------------------------------------
// Gaia DR3 -- csv, read directly from the published gzipped files
// ---------------------------------------------------------------------------

/// Empty and the literal "null" both mean absent in the Gaia CSV dumps.
///
/// Read through a string rather than with `Option<T>` directly because serde's
/// numeric parsers reject "null" outright, which would fail the whole file.
fn gaia_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    use serde::de::Error;
    let raw = String::deserialize(deserializer)?;
    if raw.is_empty() || raw == "null" {
        return Ok(None);
    }
    raw.parse().map(Some).map_err(D::Error::custom)
}

/// One source from Gaia DR3, as published in the `GaiaSource` CSV dumps.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct Gaia {
    #[serde(rename(serialize = "_id"))]
    pub source_id: i64,
    pub ra: f64,
    pub dec: f64,
    #[serde(deserialize_with = "gaia_nullable")]
    pub ra_error: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub dec_error: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub parallax: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub parallax_error: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub pm: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub pmra: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub pmra_error: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub pmdec: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub pmdec_error: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub phot_g_mean_mag: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub phot_bp_mean_mag: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub phot_rp_mean_mag: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub ruwe: Option<f64>,
    #[serde(deserialize_with = "gaia_nullable")]
    pub phot_bp_rp_excess_factor: Option<f64>,
}

impl HasCoordinates for Gaia {}

// ---------------------------------------------------------------------------
// GALEX -- csv, read directly from the published GUVcat AIS files
// ---------------------------------------------------------------------------

/// GALEX writes -999 rather than leaving a magnitude blank.
///
/// Stored as an explicit absence: -999 as a magnitude would be an absurdly
/// bright source, and a crossmatch has no way to tell it from a real one.
fn galex_sentinel<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<f64> = gaia_nullable(deserializer)?;
    Ok(value.filter(|v| *v > -999.0))
}

/// One row of the GALEX GUVcat_AIS catalog.
///
/// <https://iopscience.iop.org/article/10.3847/1538-4365/aa7053/meta>
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct Galex {
    #[serde(rename(serialize = "_id"))]
    pub objid: i64,
    pub ra: f64,
    pub dec: f64,
    #[serde(deserialize_with = "galex_sentinel")]
    pub fuv_mag: Option<f64>,
    #[serde(deserialize_with = "galex_sentinel")]
    pub fuv_magerr: Option<f64>,
    #[serde(deserialize_with = "galex_sentinel")]
    pub nuv_mag: Option<f64>,
    #[serde(deserialize_with = "galex_sentinel")]
    pub nuv_magerr: Option<f64>,
    #[serde(alias = "fexptime", deserialize_with = "gaia_nullable")]
    pub fuv_exp: Option<f64>,
    #[serde(alias = "nexptime", deserialize_with = "gaia_nullable")]
    pub nuv_exp: Option<f64>,
}

impl HasCoordinates for Galex {}

// ---------------------------------------------------------------------------
// VSX -- fixed-width text
// ---------------------------------------------------------------------------

/// One row of the AAVSO International Variable Star Index.
///
/// Published as a single fixed-width `vsx.dat`. Column positions are the file's
/// own and are not self-describing, so they are named once here.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct Vsx {
    #[serde(rename(serialize = "_id"))]
    pub oid: i64,
    pub name: String,
    pub var_flag: i32,
    pub ra: f64,
    pub dec: f64,
    /// Variability types, which the file separates with `|`.
    pub types: Vec<String>,
    pub max: Option<f64>,
    pub max_band: Option<String>,
    /// Whether `min` is an amplitude rather than a magnitude.
    pub min_is_amplitude: bool,
    pub min: Option<f64>,
    pub min_band: Option<String>,
    pub epoch: Option<f64>,
    pub period: Option<f64>,
    pub spectral_type: Option<String>,
}

/// Byte offsets of each field in `vsx.dat`.
mod vsx_col {
    pub const OID: (usize, usize) = (0, 8);
    pub const NAME: (usize, usize) = (8, 40);
    pub const VAR_FLAG: (usize, usize) = (40, 42);
    pub const RA: (usize, usize) = (42, 52);
    pub const DEC: (usize, usize) = (52, 62);
    pub const TYPES: (usize, usize) = (62, 96);
    pub const MAX: (usize, usize) = (96, 105);
    pub const MAX_BAND: (usize, usize) = (105, 110);
    pub const MIN_IS_AMPLITUDE: (usize, usize) = (110, 118);
    pub const MIN: (usize, usize) = (118, 127);
    pub const MIN_BAND: (usize, usize) = (127, 137);
    pub const EPOCH: (usize, usize) = (137, 156);
    pub const PERIOD: (usize, usize) = (156, 174);
    pub const SPECTRAL_TYPE: (usize, usize) = (174, 204);
}

impl super::ascii::FromAsciiRow for Vsx {
    fn from_line(line: &str) -> Result<Self, String> {
        let field = |(start, end): (usize, usize)| column(line, start, end).trim();

        let oid = field(vsx_col::OID)
            .parse()
            .map_err(|e| format!("oid {:?}: {}", field(vsx_col::OID), e))?;
        let name = field(vsx_col::NAME);
        if name.is_empty() {
            // The name is the only human-readable identifier on the record.
            return Err("empty name".to_string());
        }
        let ra = field(vsx_col::RA)
            .parse()
            .map_err(|e| format!("ra {:?}: {}", field(vsx_col::RA), e))?;
        let dec = field(vsx_col::DEC)
            .parse()
            .map_err(|e| format!("dec {:?}: {}", field(vsx_col::DEC), e))?;

        Ok(Vsx {
            oid,
            name: name.to_string(),
            // Absent rather than fatal: the flag is metadata about the record,
            // not the star, and a blank one should not drop a real variable.
            var_flag: optional(field(vsx_col::VAR_FLAG)).unwrap_or_default(),
            ra,
            dec,
            types: optional_string(field(vsx_col::TYPES))
                .map(|t| t.split('|').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default(),
            max: optional(field(vsx_col::MAX)),
            max_band: optional_string(field(vsx_col::MAX_BAND)),
            min_is_amplitude: matches!(
                field(vsx_col::MIN_IS_AMPLITUDE)
                    .to_ascii_lowercase()
                    .as_str(),
                "1" | "true" | "yes" | "y"
            ),
            min: optional(field(vsx_col::MIN)),
            min_band: optional_string(field(vsx_col::MIN_BAND)),
            epoch: optional(field(vsx_col::EPOCH)),
            period: optional(field(vsx_col::PERIOD)),
            spectral_type: optional_string(field(vsx_col::SPECTRAL_TYPE)),
        })
    }
}

impl HasCoordinates for Vsx {}

// ---------------------------------------------------------------------------
// Pan-STARRS -- parquet, from the HATS mirror on S3
// ---------------------------------------------------------------------------

/// One object from the Pan-STARRS "otmo" (object-mean) table.
///
/// Stored keys are the survey's own camelCase column names, which the
/// crossmatch projections are written against; `raMean`/`decMean` are stored as
/// plain `ra`/`dec` so the shared coordinate handling applies.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct PanStarrs {
    #[serde(rename(serialize = "_id"))]
    pub obj_id: i64,
    pub ra: f64,
    pub dec: f64,
    #[serde(rename(serialize = "gMeanPSFMag"))]
    pub g_mean_psf_mag: Option<f64>,
    #[serde(rename(serialize = "gMeanPSFMagErr"))]
    pub g_mean_psf_mag_err: Option<f64>,
    #[serde(rename(serialize = "rMeanPSFMag"))]
    pub r_mean_psf_mag: Option<f64>,
    #[serde(rename(serialize = "rMeanPSFMagErr"))]
    pub r_mean_psf_mag_err: Option<f64>,
    #[serde(rename(serialize = "iMeanPSFMag"))]
    pub i_mean_psf_mag: Option<f64>,
    #[serde(rename(serialize = "iMeanPSFMagErr"))]
    pub i_mean_psf_mag_err: Option<f64>,
    #[serde(rename(serialize = "zMeanPSFMag"))]
    pub z_mean_psf_mag: Option<f64>,
    #[serde(rename(serialize = "zMeanPSFMagErr"))]
    pub z_mean_psf_mag_err: Option<f64>,
    #[serde(rename(serialize = "yMeanPSFMag"))]
    pub y_mean_psf_mag: Option<f64>,
    #[serde(rename(serialize = "yMeanPSFMagErr"))]
    pub y_mean_psf_mag_err: Option<f64>,
}

impl super::arrow::FromRecordBatch for PanStarrs {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column};

        let obj_id = i64_column(batch, "objID")?;
        let ra = f64_column(batch, "raMean")?;
        let dec = f64_column(batch, "decMean")?;
        let g = f64_column(batch, "gMeanPSFMag")?;
        let g_err = f64_column(batch, "gMeanPSFMagErr")?;
        let r = f64_column(batch, "rMeanPSFMag")?;
        let r_err = f64_column(batch, "rMeanPSFMagErr")?;
        let i_band = f64_column(batch, "iMeanPSFMag")?;
        let i_err = f64_column(batch, "iMeanPSFMagErr")?;
        let z = f64_column(batch, "zMeanPSFMag")?;
        let z_err = f64_column(batch, "zMeanPSFMagErr")?;
        let y = f64_column(batch, "yMeanPSFMag")?;
        let y_err = f64_column(batch, "yMeanPSFMagErr")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            // Pan-STARRS writes -999 for an unmeasured mean position, which
            // f64_column keeps as a number; a source without a real position
            // cannot be crossmatched and would fail the 2dsphere index.
            let (Some(obj_id), Some(ra), Some(dec)) = (obj_id[row], ra[row], dec[row]) else {
                continue;
            };
            if !(0.0..=360.0).contains(&ra) || !(-90.0..=90.0).contains(&dec) {
                continue;
            }
            rows.push(PanStarrs {
                obj_id,
                ra,
                dec,
                g_mean_psf_mag: g[row],
                g_mean_psf_mag_err: g_err[row],
                r_mean_psf_mag: r[row],
                r_mean_psf_mag_err: r_err[row],
                i_mean_psf_mag: i_band[row],
                i_mean_psf_mag_err: i_err[row],
                z_mean_psf_mag: z[row],
                z_mean_psf_mag_err: z_err[row],
                y_mean_psf_mag: y[row],
                y_mean_psf_mag_err: y_err[row],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for PanStarrs {}

// ---------------------------------------------------------------------------
// Legacy Survey DR9 -- parquet, merged from the published sweep and photo-z
// FITS pairs by boompy
// ---------------------------------------------------------------------------

/// The Legacy Survey composite id: `objid + (brickid << N) + (release << 40)`.
///
/// A brick id occupies 16 bits before DR10 (release < 10000) and 20 from DR10
/// on, which is the packing the `LSDR10` collection's ids were built with. The
/// release sits above both, so a DR9 and a DR10 source sharing a brick and an
/// object number still get different ids -- which is what lets a deployment
/// hold both releases without either one's documents landing on the other's.
fn legacy_survey_id(release: i64, brickid: i64, objid: i64) -> i64 {
    let shift = if release >= 10_000 { 20 } else { 16 };
    objid + (brickid << shift) + (release << 40)
}

/// One Legacy Survey DR9 source, with the photo-z columns merged in.
///
/// Field names are DR10's, because the crossmatch projections and the
/// host-galaxy association are written against those and a DR9 row has to
/// answer to the same names. `flux_i` is the exception and is absent rather
/// than null: DR9 predates the i-band, so there is no such column upstream.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct LsDr9 {
    #[serde(rename(serialize = "_id"))]
    pub id: i64,
    pub ra: f64,
    pub dec: f64,
    /// Tractor morphological type: `PSF`, `REX`, `EXP`, `DEV`, `SER` or `DUP`.
    /// Stored as `objtype` because `type` is what the sweeps call it and a
    /// document key of that name reads as a BSON type to anyone skimming.
    pub objtype: String,
    pub ebv: Option<f64>,
    pub z_spec: Option<f64>,
    /// Which survey contributed the spectroscopic redshift, absent when none
    /// did.
    pub survey: Option<String>,
    pub z_phot_mean: Option<f64>,
    pub z_phot_median: Option<f64>,
    pub z_phot_std: Option<f64>,
    pub z_phot_l95: Option<f64>,
    pub z_phot_u95: Option<f64>,
    pub flux_g: Option<f64>,
    pub flux_r: Option<f64>,
    pub flux_z: Option<f64>,
    pub flux_w1: Option<f64>,
    pub flux_w2: Option<f64>,
    pub flux_w3: Option<f64>,
    pub flux_w4: Option<f64>,
    /// Tractor ellipse: half-light radius in arcsec and the two ellipticity
    /// components. `shape_r` is 0 for point sources, which have no extent.
    pub shape_r: Option<f64>,
    pub shape_e1: Option<f64>,
    pub shape_e2: Option<f64>,
    /// Sersic index, fit only for `SER` objects. Putting the half-light radius
    /// on an isophotal scale needs it; `REX` and `EXP` are n = 1 and `DEV` is
    /// n = 4 by definition, so its absence is not a missing measurement there.
    pub sersic: Option<f64>,
    /// Per-band inverse variances. `flux_r * sqrt(flux_ivar_r)` is the r-band
    /// signal-to-noise that separates a marginal `REX` detection from a real
    /// galaxy; g and z give that cut a fallback when r is missing.
    pub flux_ivar_g: Option<f64>,
    pub flux_ivar_r: Option<f64>,
    pub flux_ivar_z: Option<f64>,
    /// Fraction of the flux in this object's aperture contributed by its
    /// neighbors. A high value marks a shredded fragment of a larger galaxy
    /// rather than a host.
    pub fracflux_g: Option<f64>,
    pub fracflux_r: Option<f64>,
    pub fracflux_z: Option<f64>,
    /// Exposures per band. A zero distinguishes "not observed in this band"
    /// from "observed and not detected", which otherwise both read as a
    /// missing flux.
    pub nobs_g: Option<i64>,
    pub nobs_r: Option<i64>,
    pub nobs_z: Option<i64>,
}

impl super::arrow::FromRecordBatch for LsDr9 {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column, string_column};

        // boompy writes exactly these columns out of each sweep and photo-z
        // pair. Naming them again here is how the reader says what it needs,
        // and a column that stopped being written fails with its own name
        // rather than becoming a collection of null fields.
        let release = i64_column(batch, "release")?;
        let brickid = i64_column(batch, "brickid")?;
        let objid = i64_column(batch, "objid")?;
        let ra = f64_column(batch, "ra")?;
        let dec = f64_column(batch, "dec")?;
        let objtype = string_column(batch, "type")?;
        let ebv = f64_column(batch, "ebv")?;
        let z_spec = f64_column(batch, "z_spec")?;
        let survey = string_column(batch, "survey")?;
        let z_phot_mean = f64_column(batch, "z_phot_mean")?;
        let z_phot_median = f64_column(batch, "z_phot_median")?;
        let z_phot_std = f64_column(batch, "z_phot_std")?;
        let z_phot_l95 = f64_column(batch, "z_phot_l95")?;
        let z_phot_u95 = f64_column(batch, "z_phot_u95")?;
        let flux_g = f64_column(batch, "flux_g")?;
        let flux_r = f64_column(batch, "flux_r")?;
        let flux_z = f64_column(batch, "flux_z")?;
        let flux_w1 = f64_column(batch, "flux_w1")?;
        let flux_w2 = f64_column(batch, "flux_w2")?;
        let flux_w3 = f64_column(batch, "flux_w3")?;
        let flux_w4 = f64_column(batch, "flux_w4")?;
        let shape_r = f64_column(batch, "shape_r")?;
        let shape_e1 = f64_column(batch, "shape_e1")?;
        let shape_e2 = f64_column(batch, "shape_e2")?;
        let sersic = f64_column(batch, "sersic")?;
        let flux_ivar_g = f64_column(batch, "flux_ivar_g")?;
        let flux_ivar_r = f64_column(batch, "flux_ivar_r")?;
        let flux_ivar_z = f64_column(batch, "flux_ivar_z")?;
        let fracflux_g = f64_column(batch, "fracflux_g")?;
        let fracflux_r = f64_column(batch, "fracflux_r")?;
        let fracflux_z = f64_column(batch, "fracflux_z")?;
        let nobs_g = i64_column(batch, "nobs_g")?;
        let nobs_r = i64_column(batch, "nobs_r")?;
        let nobs_z = i64_column(batch, "nobs_z")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            // Without all three id columns there is no stable `_id`, so a
            // resumed ingest would duplicate the row rather than overwrite it;
            // without a position it cannot be crossmatched at all. Skip either
            // rather than fabricate the missing half.
            let (Some(release), Some(brickid), Some(objid), Some(ra), Some(dec), Some(objtype)) = (
                release[row],
                brickid[row],
                objid[row],
                ra[row],
                dec[row],
                objtype[row].clone(),
            ) else {
                continue;
            };
            rows.push(LsDr9 {
                id: legacy_survey_id(release, brickid, objid),
                ra,
                dec,
                objtype,
                ebv: ebv[row],
                z_spec: z_spec[row],
                survey: survey[row].clone(),
                z_phot_mean: z_phot_mean[row],
                z_phot_median: z_phot_median[row],
                z_phot_std: z_phot_std[row],
                z_phot_l95: z_phot_l95[row],
                z_phot_u95: z_phot_u95[row],
                flux_g: flux_g[row],
                flux_r: flux_r[row],
                flux_z: flux_z[row],
                flux_w1: flux_w1[row],
                flux_w2: flux_w2[row],
                flux_w3: flux_w3[row],
                flux_w4: flux_w4[row],
                shape_r: shape_r[row],
                shape_e1: shape_e1[row],
                shape_e2: shape_e2[row],
                sersic: sersic[row],
                flux_ivar_g: flux_ivar_g[row],
                flux_ivar_r: flux_ivar_r[row],
                flux_ivar_z: flux_ivar_z[row],
                fracflux_g: fracflux_g[row],
                fracflux_r: fracflux_r[row],
                fracflux_z: fracflux_z[row],
                nobs_g: nobs_g[row],
                nobs_r: nobs_r[row],
                nobs_z: nobs_z[row],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for LsDr9 {}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct Erass1 {
    #[serde(rename(serialize = "_id"))]
    pub uid: i64,
    pub iauname: String,
    pub ra: f64,
    pub dec: f64,
    pub pos_err: Option<f64>,
    pub mjd_min: Option<f64>,
    pub mjd_max: Option<f64>,
    pub ext: Option<f64>,
    pub ext_like: Option<f64>,
    pub det_like_0: Option<f64>,
    pub ml_rate_1: Option<f64>,
    pub ml_rate_err_1: Option<f64>,
    pub ml_flux_1: Option<f64>,
    pub ml_flux_err_1: Option<f64>,
    pub ml_rate_p1: Option<f64>,
    pub ml_rate_err_p1: Option<f64>,
    pub ml_rate_p2: Option<f64>,
    pub ml_rate_err_p2: Option<f64>,
    pub ml_rate_p3: Option<f64>,
    pub ml_rate_err_p3: Option<f64>,
    pub flag_sp_snr: Option<i64>,
    pub flag_sp_bps: Option<i64>,
    pub flag_sp_scl: Option<i64>,
    pub flag_sp_lga: Option<i64>,
    pub flag_sp_gc_cons: Option<i64>,
    pub flag_opt: Option<i64>,
}

impl super::arrow::FromRecordBatch for Erass1 {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column, string_column};

        let uid = i64_column(batch, "UID")?;
        let iauname = string_column(batch, "IAUNAME")?;
        let ra = f64_column(batch, "RA")?;
        let dec = f64_column(batch, "DEC")?;
        let pos_err = f64_column(batch, "POS_ERR")?;
        let mjd_min = f64_column(batch, "MJD_MIN")?;
        let mjd_max = f64_column(batch, "MJD_MAX")?;
        let ext = f64_column(batch, "EXT")?;
        let ext_like = f64_column(batch, "EXT_LIKE")?;
        let det_like_0 = f64_column(batch, "DET_LIKE_0")?;
        let ml_rate_1 = f64_column(batch, "ML_RATE_1")?;
        let ml_rate_err_1 = f64_column(batch, "ML_RATE_ERR_1")?;
        let ml_flux_1 = f64_column(batch, "ML_FLUX_1")?;
        let ml_flux_err_1 = f64_column(batch, "ML_FLUX_ERR_1")?;
        let ml_rate_p1 = f64_column(batch, "ML_RATE_P1")?;
        let ml_rate_err_p1 = f64_column(batch, "ML_RATE_ERR_P1")?;
        let ml_rate_p2 = f64_column(batch, "ML_RATE_P2")?;
        let ml_rate_err_p2 = f64_column(batch, "ML_RATE_ERR_P2")?;
        let ml_rate_p3 = f64_column(batch, "ML_RATE_P3")?;
        let ml_rate_err_p3 = f64_column(batch, "ML_RATE_ERR_P3")?;
        let flag_sp_snr = i64_column(batch, "FLAG_SP_SNR")?;
        let flag_sp_bps = i64_column(batch, "FLAG_SP_BPS")?;
        let flag_sp_scl = i64_column(batch, "FLAG_SP_SCL")?;
        let flag_sp_lga = i64_column(batch, "FLAG_SP_LGA")?;
        let flag_sp_gc_cons = i64_column(batch, "FLAG_SP_GC_CONS")?;
        let flag_opt = i64_column(batch, "FLAG_OPT")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let (Some(uid), Some(ra), Some(dec)) = (uid[row], ra[row], dec[row]) else {
                continue;
            };
            rows.push(Erass1 {
                uid,
                iauname: iauname[row].clone().unwrap_or_default(),
                ra,
                dec,
                pos_err: pos_err[row],
                mjd_min: mjd_min[row],
                mjd_max: mjd_max[row],
                ext: ext[row],
                ext_like: ext_like[row],
                det_like_0: det_like_0[row],
                ml_rate_1: ml_rate_1[row],
                ml_rate_err_1: ml_rate_err_1[row],
                ml_flux_1: ml_flux_1[row],
                ml_flux_err_1: ml_flux_err_1[row],
                ml_rate_p1: ml_rate_p1[row],
                ml_rate_err_p1: ml_rate_err_p1[row],
                ml_rate_p2: ml_rate_p2[row],
                ml_rate_err_p2: ml_rate_err_p2[row],
                ml_rate_p3: ml_rate_p3[row],
                ml_rate_err_p3: ml_rate_err_p3[row],
                flag_sp_snr: flag_sp_snr[row],
                flag_sp_bps: flag_sp_bps[row],
                flag_sp_scl: flag_sp_scl[row],
                flag_sp_lga: flag_sp_lga[row],
                flag_sp_gc_cons: flag_sp_gc_cons[row],
                flag_opt: flag_opt[row],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for Erass1 {}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct FiveXmm {
    #[serde(rename(serialize = "_id"))]
    pub srcid: i64,
    pub iauname: String,
    pub ra: f64,
    pub dec: f64,
    pub radec_err: Option<f64>,
    pub n_obs: Option<i64>,
    pub mjd_first: Option<f64>,
    pub mjd_last: Option<f64>,
    pub ep_flux: Option<f64>,
    pub ep_flux_err: Option<f64>,
    pub ep_det_ml: Option<f64>,
    pub ep_hr1: Option<f64>,
    pub ep_hr2: Option<f64>,
    pub ep_hr3: Option<f64>,
    pub ep_hr4: Option<f64>,
    pub extent: Option<f64>,
    pub extent_ml: Option<f64>,
    pub sum_flag: Option<i64>,
    pub var_flag: Option<bool>,
    pub fvar: Option<f64>,
    pub var_prob: Option<f64>,
    pub fratio: Option<f64>,
    pub classx_class: Option<String>,
    pub classx_prob_agn: Option<f64>,
    pub classx_prob_star: Option<f64>,
    pub classx_prob_gal_xrb: Option<f64>,
    pub classx_prob_gal_cv: Option<f64>,
}

impl super::arrow::FromRecordBatch for FiveXmm {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{bool_column, f64_column, i64_column, string_column};

        let srcid = i64_column(batch, "SRCID")?;
        let iauname = string_column(batch, "IAUNAME")?;
        let ra = f64_column(batch, "RA")?;
        let dec = f64_column(batch, "DEC")?;
        let radec_err = f64_column(batch, "RADEC_ERR")?;
        let n_obs = i64_column(batch, "N_OBS")?;
        let mjd_first = f64_column(batch, "MJD_FIRST")?;
        let mjd_last = f64_column(batch, "MJD_LAST")?;
        let ep_flux = f64_column(batch, "EP_FLUX")?;
        let ep_flux_err = f64_column(batch, "EP_FLUX_ERR")?;
        let ep_det_ml = f64_column(batch, "EP_DET_ML")?;
        let ep_hr1 = f64_column(batch, "EP_HR1")?;
        let ep_hr2 = f64_column(batch, "EP_HR2")?;
        let ep_hr3 = f64_column(batch, "EP_HR3")?;
        let ep_hr4 = f64_column(batch, "EP_HR4")?;
        let extent = f64_column(batch, "EXTENT")?;
        let extent_ml = f64_column(batch, "EXTENT_ML")?;
        let sum_flag = i64_column(batch, "SUM_FLAG")?;
        let var_flag = bool_column(batch, "VAR_FLAG")?;
        let fvar = f64_column(batch, "FVAR")?;
        let var_prob = f64_column(batch, "VAR_PROB")?;
        let fratio = f64_column(batch, "FRATIO")?;
        let classx_class = string_column(batch, "CLASSX_CLASS")?;
        let classx_prob_agn = f64_column(batch, "CLASSX_PROB_AGN")?;
        let classx_prob_star = f64_column(batch, "CLASSX_PROB_STAR")?;
        let classx_prob_gal_xrb = f64_column(batch, "CLASSX_PROB_GAL_XRB")?;
        let classx_prob_gal_cv = f64_column(batch, "CLASSX_PROB_GAL_CV")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let (Some(srcid), Some(ra), Some(dec)) = (srcid[row], ra[row], dec[row]) else {
                continue;
            };
            rows.push(FiveXmm {
                srcid,
                iauname: iauname[row].clone().unwrap_or_default(),
                ra,
                dec,
                radec_err: radec_err[row],
                n_obs: n_obs[row],
                mjd_first: mjd_first[row],
                mjd_last: mjd_last[row],
                ep_flux: ep_flux[row],
                ep_flux_err: ep_flux_err[row],
                ep_det_ml: ep_det_ml[row],
                ep_hr1: ep_hr1[row],
                ep_hr2: ep_hr2[row],
                ep_hr3: ep_hr3[row],
                ep_hr4: ep_hr4[row],
                extent: extent[row],
                extent_ml: extent_ml[row],
                sum_flag: sum_flag[row],
                var_flag: var_flag[row],
                fvar: fvar[row],
                var_prob: var_prob[row],
                fratio: fratio[row],
                classx_class: classx_class[row].clone().filter(|s| !s.is_empty()),
                classx_prob_agn: classx_prob_agn[row],
                classx_prob_star: classx_prob_star[row],
                classx_prob_gal_xrb: classx_prob_gal_xrb[row],
                classx_prob_gal_cv: classx_prob_gal_cv[row],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for FiveXmm {}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct TwoSxps {
    #[serde(rename(serialize = "_id"))]
    pub id: i64,
    #[serde(rename(serialize = "IAUName"))]
    pub iau_name: String,
    pub ra: f64,
    pub dec: f64,
    #[serde(rename(serialize = "Err90"))]
    pub err90: Option<f64>,
    #[serde(rename(serialize = "AstromType"))]
    pub astrom_type: Option<i64>,
    #[serde(rename(serialize = "DetFlag"))]
    pub det_flag: Option<i64>,
    #[serde(rename(serialize = "FieldFlag"))]
    pub field_flag: Option<i64>,
    #[serde(rename(serialize = "NumObs"))]
    pub num_obs: Option<i64>,
    #[serde(rename(serialize = "NumDetObs"))]
    pub num_det_obs: Option<i64>,
    #[serde(rename(serialize = "FirstDetDate"))]
    pub first_det_date: Option<String>,
    #[serde(rename(serialize = "LastDetDate"))]
    pub last_det_date: Option<String>,
    #[serde(rename(serialize = "Rate_band0"))]
    pub rate_band0: Option<f64>,
    #[serde(rename(serialize = "Rate_band0_pos"))]
    pub rate_band0_pos: Option<f64>,
    #[serde(rename(serialize = "Rate_band0_neg"))]
    pub rate_band0_neg: Option<f64>,
    #[serde(rename(serialize = "PeakRate_band0"))]
    pub peak_rate_band0: Option<f64>,
    #[serde(rename(serialize = "HR1"))]
    pub hr1: Option<f64>,
    #[serde(rename(serialize = "HR2"))]
    pub hr2: Option<f64>,
    #[serde(rename(serialize = "PvarPchiSnapshot_band0"))]
    pub pvar_pchi_snapshot_band0: Option<f64>,
    #[serde(rename(serialize = "PvarPchiObsID_band0"))]
    pub pvar_pchi_obsid_band0: Option<f64>,
    #[serde(rename(serialize = "PowFlux"))]
    pub pow_flux: Option<f64>,
    #[serde(rename(serialize = "PowFlux_pos"))]
    pub pow_flux_pos: Option<f64>,
    #[serde(rename(serialize = "PowFlux_neg"))]
    pub pow_flux_neg: Option<f64>,
}

impl super::arrow::FromRecordBatch for TwoSxps {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column, string_column};

        let id = i64_column(batch, "2SXPS_ID")?;
        let iau_name = string_column(batch, "IAUName")?;
        let ra = f64_column(batch, "RA")?;
        let dec = f64_column(batch, "Decl")?;
        let err90 = f64_column(batch, "Err90")?;
        let astrom_type = i64_column(batch, "AstromType")?;
        let det_flag = i64_column(batch, "DetFlag")?;
        let field_flag = i64_column(batch, "FieldFlag")?;
        let num_obs = i64_column(batch, "NumObs")?;
        let num_det_obs = i64_column(batch, "NumDetObs")?;
        let first_det_date = string_column(batch, "FirstDetDate")?;
        let last_det_date = string_column(batch, "LastDetDate")?;
        let rate_band0 = f64_column(batch, "Rate_band0")?;
        let rate_band0_pos = f64_column(batch, "Rate_band0_pos")?;
        let rate_band0_neg = f64_column(batch, "Rate_band0_neg")?;
        let peak_rate_band0 = f64_column(batch, "PeakRate_band0")?;
        let hr1 = f64_column(batch, "HR1")?;
        let hr2 = f64_column(batch, "HR2")?;
        let pvar_pchi_snapshot_band0 = f64_column(batch, "PvarPchiSnapshot_band0")?;
        let pvar_pchi_obsid_band0 = f64_column(batch, "PvarPchiObsID_band0")?;
        let pow_flux = f64_column(batch, "PowFlux")?;
        let pow_flux_pos = f64_column(batch, "PowFlux_pos")?;
        let pow_flux_neg = f64_column(batch, "PowFlux_neg")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let (Some(id), Some(ra), Some(dec)) = (id[row], ra[row], dec[row]) else {
                continue;
            };
            rows.push(TwoSxps {
                id,
                iau_name: iau_name[row].clone().unwrap_or_default(),
                ra,
                dec,
                err90: err90[row],
                astrom_type: astrom_type[row],
                det_flag: det_flag[row],
                field_flag: field_flag[row],
                num_obs: num_obs[row],
                num_det_obs: num_det_obs[row],
                first_det_date: first_det_date[row].clone().filter(|s| !s.is_empty()),
                last_det_date: last_det_date[row].clone().filter(|s| !s.is_empty()),
                rate_band0: rate_band0[row],
                rate_band0_pos: rate_band0_pos[row],
                rate_band0_neg: rate_band0_neg[row],
                peak_rate_band0: peak_rate_band0[row],
                hr1: hr1[row],
                hr2: hr2[row],
                pvar_pchi_snapshot_band0: pvar_pchi_snapshot_band0[row],
                pvar_pchi_obsid_band0: pvar_pchi_obsid_band0[row],
                pow_flux: pow_flux[row],
                pow_flux_pos: pow_flux_pos[row],
                pow_flux_neg: pow_flux_neg[row],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for TwoSxps {}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct FourFgl {
    #[serde(rename(serialize = "_id"))]
    pub source_name: String,
    pub ra: f64,
    pub dec: f64,
    #[serde(rename(serialize = "DataRelease"))]
    pub data_release: Option<i64>,
    #[serde(rename(serialize = "Conf_95_SemiMajor"))]
    pub conf_95_semi_major: Option<f64>,
    #[serde(rename(serialize = "Conf_95_SemiMinor"))]
    pub conf_95_semi_minor: Option<f64>,
    #[serde(rename(serialize = "Conf_95_PosAng"))]
    pub conf_95_pos_ang: Option<f64>,
    #[serde(rename(serialize = "Signif_Avg"))]
    pub signif_avg: Option<f64>,
    #[serde(rename(serialize = "Flux1000"))]
    pub flux1000: Option<f64>,
    #[serde(rename(serialize = "Unc_Flux1000"))]
    pub unc_flux1000: Option<f64>,
    #[serde(rename(serialize = "Energy_Flux100"))]
    pub energy_flux100: Option<f64>,
    #[serde(rename(serialize = "Unc_Energy_Flux100"))]
    pub unc_energy_flux100: Option<f64>,
    #[serde(rename(serialize = "SpectrumType"))]
    pub spectrum_type: Option<String>,
    #[serde(rename(serialize = "PL_Index"))]
    pub pl_index: Option<f64>,
    #[serde(rename(serialize = "Variability_Index"))]
    pub variability_index: Option<f64>,
    #[serde(rename(serialize = "Frac_Variability"))]
    pub frac_variability: Option<f64>,
    #[serde(rename(serialize = "Flags"))]
    pub flags: Option<i64>,
    #[serde(rename(serialize = "CLASS1"))]
    pub class1: Option<String>,
    #[serde(rename(serialize = "CLASS2"))]
    pub class2: Option<String>,
    #[serde(rename(serialize = "ASSOC1"))]
    pub assoc1: Option<String>,
    #[serde(rename(serialize = "ASSOC2"))]
    pub assoc2: Option<String>,
    #[serde(rename(serialize = "ASSOC_PROB_BAY"))]
    pub assoc_prob_bay: Option<f64>,
    #[serde(rename(serialize = "ASSOC_PROB_LR"))]
    pub assoc_prob_lr: Option<f64>,
    #[serde(rename(serialize = "RA_Counterpart"))]
    pub ra_counterpart: Option<f64>,
    #[serde(rename(serialize = "DEC_Counterpart"))]
    pub dec_counterpart: Option<f64>,
}

impl super::arrow::FromRecordBatch for FourFgl {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column, string_column};

        let source_name = string_column(batch, "Source_Name")?;
        let ra = f64_column(batch, "RAJ2000")?;
        let dec = f64_column(batch, "DEJ2000")?;
        let data_release = i64_column(batch, "DataRelease")?;
        let conf_95_semi_major = f64_column(batch, "Conf_95_SemiMajor")?;
        let conf_95_semi_minor = f64_column(batch, "Conf_95_SemiMinor")?;
        let conf_95_pos_ang = f64_column(batch, "Conf_95_PosAng")?;
        let signif_avg = f64_column(batch, "Signif_Avg")?;
        let flux1000 = f64_column(batch, "Flux1000")?;
        let unc_flux1000 = f64_column(batch, "Unc_Flux1000")?;
        let energy_flux100 = f64_column(batch, "Energy_Flux100")?;
        let unc_energy_flux100 = f64_column(batch, "Unc_Energy_Flux100")?;
        let spectrum_type = string_column(batch, "SpectrumType")?;
        let pl_index = f64_column(batch, "PL_Index")?;
        let variability_index = f64_column(batch, "Variability_Index")?;
        let frac_variability = f64_column(batch, "Frac_Variability")?;
        let flags = i64_column(batch, "Flags")?;
        let class1 = string_column(batch, "CLASS1")?;
        let class2 = string_column(batch, "CLASS2")?;
        let assoc1 = string_column(batch, "ASSOC1")?;
        let assoc2 = string_column(batch, "ASSOC2")?;
        let assoc_prob_bay = f64_column(batch, "ASSOC_PROB_BAY")?;
        let assoc_prob_lr = f64_column(batch, "ASSOC_PROB_LR")?;
        let ra_counterpart = f64_column(batch, "RA_Counterpart")?;
        let dec_counterpart = f64_column(batch, "DEC_Counterpart")?;

        let text = |v: &Option<String>| v.clone().filter(|s| !s.is_empty());

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let (Some(source_name), Some(ra), Some(dec)) =
                (source_name[row].clone(), ra[row], dec[row])
            else {
                continue;
            };
            rows.push(FourFgl {
                source_name,
                ra,
                dec,
                data_release: data_release[row],
                conf_95_semi_major: conf_95_semi_major[row],
                conf_95_semi_minor: conf_95_semi_minor[row],
                conf_95_pos_ang: conf_95_pos_ang[row],
                signif_avg: signif_avg[row],
                flux1000: flux1000[row],
                unc_flux1000: unc_flux1000[row],
                energy_flux100: energy_flux100[row],
                unc_energy_flux100: unc_energy_flux100[row],
                spectrum_type: text(&spectrum_type[row]),
                pl_index: pl_index[row],
                variability_index: variability_index[row],
                frac_variability: frac_variability[row],
                flags: flags[row],
                class1: text(&class1[row]),
                class2: text(&class2[row]),
                assoc1: text(&assoc1[row]),
                assoc2: text(&assoc2[row]),
                assoc_prob_bay: assoc_prob_bay[row],
                assoc_prob_lr: assoc_prob_lr[row],
                ra_counterpart: ra_counterpart[row],
                dec_counterpart: dec_counterpart[row],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for FourFgl {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogs::ascii::FromAsciiRow;

    /// A 2MASS PSC record with the fields this reader uses filled in and the
    /// rest blank, at the published width.
    fn psc_line(overrides: &[(usize, &str)]) -> String {
        let mut fields = vec![""; twomass_col::COUNT];
        fields[twomass_col::RA] = "12.345678";
        fields[twomass_col::DEC] = "-9.876543";
        fields[twomass_col::DESIGNATION] = "00494236-0552438";
        fields[twomass_col::J_M] = "15.123";
        fields[twomass_col::J_M + 12] = "AAA"; // ph_qual
        fields[twomass_col::NDET] = "666";
        for (index, value) in overrides {
            fields[*index] = value;
        }
        fields.join("|")
    }

    #[test]
    fn twomass_parses_a_published_record() {
        let record = TwoMass::from_line(&psc_line(&[])).expect("should parse");
        assert_eq!(record.designation, "00494236-0552438");
        assert_eq!(record.ra, 12.345678);
        assert_eq!(record.dec, -9.876543);
        assert_eq!(record.j_m, Some(15.123));
        assert_eq!(record.ph_qual.as_deref(), Some("AAA"));
        assert_eq!(record.ndet, Some(666));
    }

    #[test]
    fn twomass_reads_blank_photometry_as_absent() {
        // The PSC leaves a band blank rather than writing a sentinel, and a
        // magnitude parsed as 0.0 would be a very bright source.
        let record =
            TwoMass::from_line(&psc_line(&[(twomass_col::J_M, "   ")])).expect("should parse");
        assert_eq!(record.j_m, None);
    }

    #[test]
    fn twomass_rejects_a_truncated_line() {
        let short = "12.3|4.5|not|enough|fields";
        assert!(TwoMass::from_line(short)
            .unwrap_err()
            .contains("expected 60 fields"));
    }

    #[test]
    fn twomass_rejects_an_unparseable_position() {
        // Ingesting this with a defaulted position would place a real source at
        // the wrong point on the sky, which a crossmatch cannot detect.
        let err = TwoMass::from_line(&psc_line(&[(twomass_col::RA, "null")])).unwrap_err();
        assert!(err.starts_with("ra "), "{err}");
    }

    #[test]
    fn twomass_rejects_a_record_with_no_designation() {
        // The designation becomes `_id`; an empty one would collapse every such
        // record onto a single document.
        let err = TwoMass::from_line(&psc_line(&[(twomass_col::DESIGNATION, " ")])).unwrap_err();
        assert_eq!(err, "empty designation");
    }

    /// A vsx.dat line built from the file's own column offsets.
    fn vsx_line(overrides: &[((usize, usize), &str)]) -> String {
        let mut line = vec![b' '; 204];
        // Blank the whole field before writing, or an override shorter than
        // the value it replaces leaves the previous tail behind.
        let mut put = |(start, end): (usize, usize), value: &str| {
            line[start..end].fill(b' ');
            let bytes = value.as_bytes();
            let width = (end - start).min(bytes.len());
            line[start..start + width].copy_from_slice(&bytes[..width]);
        };
        put(vsx_col::OID, "12345");
        put(vsx_col::NAME, "RR Lyr");
        put(vsx_col::VAR_FLAG, "0");
        put(vsx_col::RA, "291.36630");
        put(vsx_col::DEC, "42.784390");
        put(vsx_col::TYPES, "RRAB|SR");
        put(vsx_col::MAX, "7.06");
        put(vsx_col::PERIOD, "0.566783");
        for (range, value) in overrides {
            put(*range, value);
        }
        String::from_utf8(line).expect("ascii")
    }

    #[test]
    fn vsx_parses_a_published_row() {
        let record = Vsx::from_line(&vsx_line(&[])).expect("should parse");
        assert_eq!(record.oid, 12345);
        assert_eq!(record.name, "RR Lyr");
        assert_eq!(record.ra, 291.36630);
        assert_eq!(record.dec, 42.784390);
        assert_eq!(record.period, Some(0.566783));
    }

    #[test]
    fn vsx_splits_the_pipe_separated_types() {
        // A star can carry several variability classifications, and storing
        // "RRAB|SR" as one string would make a type query miss it.
        let record = Vsx::from_line(&vsx_line(&[])).expect("should parse");
        assert_eq!(record.types, vec!["RRAB", "SR"]);
    }

    #[test]
    fn vsx_reads_a_blank_optional_column_as_absent() {
        let record = Vsx::from_line(&vsx_line(&[(vsx_col::MAX, "   ")])).expect("should parse");
        assert_eq!(record.max, None);
    }

    #[test]
    fn vsx_rejects_an_unparseable_position() {
        let err = Vsx::from_line(&vsx_line(&[(vsx_col::RA, "not-a-number")])).unwrap_err();
        assert!(err.starts_with("ra "), "{err}");
    }

    #[test]
    fn vsx_tolerates_a_line_shorter_than_the_declared_width() {
        // A truncated final line should be a parse error naming the field, not
        // a panic that takes the whole chunk down.
        let short = "12345   RR Lyr";
        assert!(Vsx::from_line(short).is_err());
    }

    #[test]
    fn panstarrs_document_uses_the_surveys_own_column_names() {
        // The crossmatch projections are written against these camelCase keys,
        // so renaming them to a house style would silently empty a projection.
        let doc = mongodb::bson::to_document(&PanStarrs {
            obj_id: 1,
            ra: 10.0,
            dec: 20.0,
            g_mean_psf_mag: Some(19.0),
            g_mean_psf_mag_err: None,
            r_mean_psf_mag: None,
            r_mean_psf_mag_err: None,
            i_mean_psf_mag: None,
            i_mean_psf_mag_err: None,
            z_mean_psf_mag: None,
            z_mean_psf_mag_err: None,
            y_mean_psf_mag: None,
            y_mean_psf_mag_err: None,
        })
        .expect("serializes");
        assert!(doc.contains_key("_id"));
        assert!(doc.contains_key("gMeanPSFMag"));
        // raMean/decMean are stored plainly so the shared coordinate handling
        // in `ingest` finds them.
        assert!(doc.contains_key("ra") && doc.contains_key("dec"));
        assert!(!doc.contains_key("raMean"));
    }

    #[test]
    fn ned_document_keeps_absent_values_as_explicit_nulls() {
        // Not skip_serializing_none, deliberately: consumers project these keys
        // and error when they are missing entirely.
        let doc = mongodb::bson::to_document(&Ned::default()).expect("serializes");
        assert_eq!(doc.get("z"), Some(&mongodb::bson::Bson::Null));
        assert!(doc.contains_key("DistMpc"));
        assert!(doc.contains_key("Diam_ba"));
    }

    #[test]
    fn allwise_document_keys_match_the_published_column_names() {
        // boompy projects exactly these columns out of the HATS catalog, so a
        // field added to the struct without adding the column here would read
        // back as missing.
        let doc = mongodb::bson::to_document(&AllWise {
            source_id: "x".into(),
            ra: 1.0,
            dec: 2.0,
            sigra: 0.1,
            sigdec: 0.1,
            w1mpro: Some(1.0),
            w2mpro: Some(1.0),
            w3mpro: Some(1.0),
            w4mpro: Some(1.0),
            w1sigmpro: Some(1.0),
            w2sigmpro: Some(1.0),
            w3sigmpro: Some(1.0),
            w4sigmpro: Some(1.0),
            w1rchi2: Some(1.0),
            w2rchi2: Some(1.0),
            pmra: Some(1.0),
            pmdec: Some(1.0),
            sigpmra: Some(1.0),
            sigpmdec: Some(1.0),
        })
        .expect("serializes");
        // source_id is stored as _id; the other 18 keep their published names,
        // which the crossmatch projections in config.yaml are written against.
        assert!(doc.contains_key("_id"));
        assert!(!doc.contains_key("source_id"));
        assert!(doc.contains_key("w4sigmpro"));
        assert_eq!(doc.len(), 19);
    }

    /// Every record type must serialize an `_id` taken from a source
    /// identifier.
    ///
    /// The ingest's whole resume story rests on this: an interrupted chunk is
    /// re-ingested from the start, and what makes that safe rather than
    /// duplicating is that each record lands on the same `_id` and the
    /// duplicate-key error is read as "already there". A record type that let
    /// Mongo generate an `ObjectId` instead would resume by inserting every
    /// row again, silently, with no error anywhere.
    ///
    /// Reads the source rather than instances because there is nothing
    /// generic to iterate: the types share no trait, and a twelfth catalog is
    /// added by writing a twelfth struct.
    /// One DR9 row with every column boompy writes, filled with plausible
    /// values; the names in `absent` are written as nulls instead.
    fn lsdr9_batch(absent: &[&str]) -> ::arrow::array::RecordBatch {
        use ::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
        use ::arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        const INTS: [(&str, i64); 6] = [
            ("release", 9011),
            ("brickid", 7),
            ("objid", 3),
            ("nobs_g", 2),
            ("nobs_r", 3),
            ("nobs_z", 0),
        ];
        const STRINGS: [(&str, &str); 2] = [("type", "SER"), ("survey", "SDSS")];
        const FLOATS: [(&str, f64); 26] = [
            ("ra", 10.5),
            ("dec", 40.25),
            ("ebv", 0.02),
            ("z_spec", 0.11),
            ("z_phot_mean", 0.12),
            ("z_phot_median", 0.13),
            ("z_phot_std", 0.01),
            ("z_phot_l95", 0.05),
            ("z_phot_u95", 0.2),
            ("flux_g", 10.0),
            ("flux_r", 20.0),
            ("flux_z", 30.0),
            ("flux_w1", 40.0),
            ("flux_w2", 41.0),
            ("flux_w3", 42.0),
            ("flux_w4", 43.0),
            ("shape_r", 1.5),
            ("shape_e1", 0.1),
            ("shape_e2", -0.2),
            ("sersic", 2.0),
            ("flux_ivar_g", 100.0),
            ("flux_ivar_r", 200.0),
            ("flux_ivar_z", 300.0),
            ("fracflux_g", 0.01),
            ("fracflux_r", 0.02),
            ("fracflux_z", 0.03),
        ];

        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (name, value) in INTS {
            let value = (!absent.contains(&name)).then_some(value);
            fields.push(Field::new(name, ::arrow::datatypes::DataType::Int64, true));
            columns.push(Arc::new(Int64Array::from(vec![value])));
        }
        for (name, value) in STRINGS {
            let value = (!absent.contains(&name)).then_some(value);
            fields.push(Field::new(name, ::arrow::datatypes::DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![value])));
        }
        for (name, value) in FLOATS {
            let value = (!absent.contains(&name)).then_some(value);
            fields.push(Field::new(
                name,
                ::arrow::datatypes::DataType::Float64,
                true,
            ));
            columns.push(Arc::new(Float64Array::from(vec![value])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch")
    }

    fn one_lsdr9_row(absent: &[&str]) -> Vec<LsDr9> {
        use crate::catalogs::arrow::FromRecordBatch;
        LsDr9::from_batch(&lsdr9_batch(absent)).expect("every column is present")
    }

    #[test]
    fn lsdr9_id_keeps_the_releases_apart() {
        // Same brick and object number in two releases: the shift rule is what
        // makes those two different documents rather than one overwriting the
        // other if a deployment holds both.
        assert_ne!(
            legacy_survey_id(9011, 7, 3),
            legacy_survey_id(10000, 7, 3),
            "DR9 and DR10 ids collide"
        );
        // The survey's own rule: 16 bits for the brick before DR10, 20 from
        // DR10 on, and the release above both.
        assert_eq!(legacy_survey_id(9011, 1, 3), 3 + (1 << 16) + (9011 << 40));
        assert_eq!(legacy_survey_id(10000, 1, 3), 3 + (1 << 20) + (10000 << 40));
    }

    #[test]
    fn lsdr9_document_keys_are_the_ones_the_crossmatch_projections_read() {
        // These are DR10's column names, and config.yaml's LSDR10 projection
        // plus the host-galaxy association are written against them. A DR9 row
        // that answered to different names would store fine and then project
        // to nothing.
        let doc = mongodb::bson::to_document(&one_lsdr9_row(&[])[0]).expect("serializes");
        for key in [
            "_id",
            "ra",
            "dec",
            "objtype",
            "ebv",
            "z_spec",
            "z_phot_mean",
            "z_phot_median",
            "shape_r",
            "shape_e1",
            "shape_e2",
            "sersic",
            "flux_r",
            "flux_ivar_r",
            "fracflux_r",
            "nobs_r",
        ] {
            assert!(doc.contains_key(key), "{key} is missing from the document");
        }
        // `type` is the sweep's name for it, and would read as a BSON type.
        assert!(!doc.contains_key("type"));
        // DR9 predates the i-band, so this is absent rather than null.
        assert!(!doc.contains_key("flux_i"));
        assert_eq!(doc.get_str("objtype").ok(), Some("SER"));
        assert_eq!(doc.get_i64("_id").ok(), Some(legacy_survey_id(9011, 7, 3)));
    }

    #[test]
    fn desi_omits_a_quality_figure_the_pipeline_did_not_report() {
        // Not `NaN`: MongoDB sorts NaN below every number, so a cut like
        // `{zerr: {$lt: 0.001}}` -- and `zerr` is projected by all three
        // shipped crossmatch configs -- would match rows whose redshift error
        // was never measured.
        let row = DesiDr1 {
            targetid: 1,
            ra: 10.0,
            dec: 20.0,
            survey: "main".into(),
            program: "dark".into(),
            z: 0.5,
            zerr: None,
            zwarn: 0,
            chi2: None,
            deltachi2: None,
            spectype: "GALAXY".into(),
            subtype: None,
            zcat_nspec: 1,
        };
        let doc = mongodb::bson::to_document(&row).expect("serializes");
        for absent in ["zerr", "chi2", "deltachi2"] {
            assert!(!doc.contains_key(absent), "{absent} was stored");
        }
        assert_eq!(doc.get_f64("z").expect("z is stored"), 0.5);
    }

    #[test]
    fn lsdr9_omits_a_measurement_the_survey_does_not_have() {
        // boompy turns the photo-z sweeps' -99 into a null. Storing it as a key
        // with no value would be read as a redshift by anything that projects
        // the field and checks only for its presence.
        let doc = mongodb::bson::to_document(&one_lsdr9_row(&["z_spec", "survey"])[0])
            .expect("serializes");
        assert!(!doc.contains_key("z_spec"));
        assert!(!doc.contains_key("survey"));
        assert!(doc.contains_key("z_phot_mean"));
    }

    #[test]
    fn lsdr9_skips_a_row_that_cannot_be_identified_or_placed() {
        // No id means a resumed chunk inserts the row again instead of
        // overwriting it; no position means nothing can crossmatch against it.
        for absent in [
            vec!["objid"],
            vec!["brickid"],
            vec!["release"],
            vec!["ra"],
            vec!["dec"],
            vec!["type"],
        ] {
            assert!(
                one_lsdr9_row(&absent).is_empty(),
                "a row with no {absent:?} was kept"
            );
        }
    }

    fn one_row(ints: &[&str], floats: &[&str], strings: &[&str]) -> ::arrow::array::RecordBatch {
        use ::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
        use ::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for name in ints {
            fields.push(Field::new(*name, DataType::Int64, true));
            columns.push(Arc::new(Int64Array::from(vec![1])));
        }
        for name in floats {
            fields.push(Field::new(*name, DataType::Float64, true));
            columns.push(Arc::new(Float64Array::from(vec![1.0])));
        }
        for name in strings {
            fields.push(Field::new(*name, DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec!["x"])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch")
    }

    #[test]
    fn high_energy_documents_have_every_key_their_crossmatch_projects() {
        use crate::catalogs::arrow::FromRecordBatch;
        use mongodb::bson::Document;

        fn first<T: FromRecordBatch + serde::Serialize>(
            batch: ::arrow::array::RecordBatch,
        ) -> Document {
            let rows = T::from_batch(&batch).expect("every column is present");
            mongodb::bson::to_document(&rows[0]).expect("serializes")
        }

        let documents = [
            (
                "eRASS1_Main",
                first::<Erass1>(one_row(
                    &[
                        "UID",
                        "FLAG_SP_SNR",
                        "FLAG_SP_BPS",
                        "FLAG_SP_SCL",
                        "FLAG_SP_LGA",
                        "FLAG_SP_GC_CONS",
                        "FLAG_OPT",
                    ],
                    &[
                        "RA",
                        "DEC",
                        "POS_ERR",
                        "MJD_MIN",
                        "MJD_MAX",
                        "EXT",
                        "EXT_LIKE",
                        "DET_LIKE_0",
                        "ML_RATE_1",
                        "ML_RATE_ERR_1",
                        "ML_FLUX_1",
                        "ML_FLUX_ERR_1",
                        "ML_RATE_P1",
                        "ML_RATE_ERR_P1",
                        "ML_RATE_P2",
                        "ML_RATE_ERR_P2",
                        "ML_RATE_P3",
                        "ML_RATE_ERR_P3",
                    ],
                    &["IAUNAME"],
                )),
            ),
            (
                "5XMM_DR15",
                first::<FiveXmm>(one_row(
                    &["SRCID", "N_OBS", "SUM_FLAG", "VAR_FLAG"],
                    &[
                        "RA",
                        "DEC",
                        "RADEC_ERR",
                        "MJD_FIRST",
                        "MJD_LAST",
                        "EP_FLUX",
                        "EP_FLUX_ERR",
                        "EP_DET_ML",
                        "EP_HR1",
                        "EP_HR2",
                        "EP_HR3",
                        "EP_HR4",
                        "EXTENT",
                        "EXTENT_ML",
                        "FVAR",
                        "VAR_PROB",
                        "FRATIO",
                        "CLASSX_PROB_AGN",
                        "CLASSX_PROB_STAR",
                        "CLASSX_PROB_GAL_XRB",
                        "CLASSX_PROB_GAL_CV",
                    ],
                    &["IAUNAME", "CLASSX_CLASS"],
                )),
            ),
            (
                "2SXPS",
                first::<TwoSxps>(one_row(
                    &[
                        "2SXPS_ID",
                        "AstromType",
                        "DetFlag",
                        "FieldFlag",
                        "NumObs",
                        "NumDetObs",
                    ],
                    &[
                        "RA",
                        "Decl",
                        "Err90",
                        "Rate_band0",
                        "Rate_band0_pos",
                        "Rate_band0_neg",
                        "PeakRate_band0",
                        "HR1",
                        "HR2",
                        "PvarPchiSnapshot_band0",
                        "PvarPchiObsID_band0",
                        "PowFlux",
                        "PowFlux_pos",
                        "PowFlux_neg",
                    ],
                    &["IAUName", "FirstDetDate", "LastDetDate"],
                )),
            ),
            (
                "4FGL_DR4",
                first::<FourFgl>(one_row(
                    &["DataRelease", "Flags"],
                    &[
                        "RAJ2000",
                        "DEJ2000",
                        "Conf_95_SemiMajor",
                        "Conf_95_SemiMinor",
                        "Conf_95_PosAng",
                        "Signif_Avg",
                        "Flux1000",
                        "Unc_Flux1000",
                        "Energy_Flux100",
                        "Unc_Energy_Flux100",
                        "PL_Index",
                        "Variability_Index",
                        "Frac_Variability",
                        "ASSOC_PROB_BAY",
                        "ASSOC_PROB_LR",
                        "RA_Counterpart",
                        "DEC_Counterpart",
                    ],
                    &[
                        "Source_Name",
                        "SpectrumType",
                        "CLASS1",
                        "CLASS2",
                        "ASSOC1",
                        "ASSOC2",
                    ],
                )),
            ),
        ];

        let config =
            crate::catalogs::config_from(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yaml"));
        for (collection, doc) in &documents {
            let entries: Vec<_> = config
                .crossmatch
                .values()
                .flatten()
                .filter(|entry| entry.catalog == *collection)
                .collect();
            assert!(
                !entries.is_empty(),
                "config.yaml does not crossmatch {collection}"
            );
            for entry in entries {
                for key in entry.projection.keys() {
                    assert!(
                        doc.contains_key(key),
                        "{collection} projects {key}, which its documents do not have"
                    );
                }
            }
        }
    }

    #[test]
    fn every_record_type_serializes_an_id() {
        let source = include_str!("types.rs");
        let mut structs = Vec::new();
        let mut current: Option<(&str, bool)> = None;
        for line in source.lines() {
            if let Some(rest) = line.strip_prefix("pub struct ") {
                let name = rest.trim_end_matches(" {").trim_end_matches(';');
                current = Some((name, false));
                continue;
            }
            if let Some((name, found)) = current {
                if line.contains(r#""_id""#) {
                    current = Some((name, true));
                } else if line == "}" {
                    structs.push((name, found));
                    current = None;
                }
            }
        }

        assert!(
            structs.len() >= 11,
            "expected a record type per catalog, found {}",
            structs.len()
        );
        let missing: Vec<&str> = structs
            .iter()
            .filter(|(_, found)| !found)
            .map(|(name, _)| *name)
            .collect();
        assert!(
            missing.is_empty(),
            "these record types do not serialize an _id, so a resumed ingest \
             would duplicate every row: {missing:?}"
        );
    }
}
