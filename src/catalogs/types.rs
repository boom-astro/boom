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
    pub zerr: f64,
    pub zwarn: i64,
    pub chi2: f64,
    pub deltachi2: f64,
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
                zerr: zerr[i].unwrap_or(f64::NAN),
                zwarn: zwarn[i].unwrap_or_default(),
                chi2: chi2[i].unwrap_or(f64::NAN),
                deltachi2: deltachi2[i].unwrap_or(f64::NAN),
                spectype: spectype[i].clone().unwrap_or_default(),
                subtype: subtype[i].clone().filter(|s| !s.is_empty()),
                zcat_nspec: zcat_nspec[i].unwrap_or_default(),
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for DesiDr1 {}

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
// Legacy Survey DR10 photo-z -- staged parquet, built offline
// ---------------------------------------------------------------------------

/// One source from the Legacy Survey DR10 tractor catalog, with its photo-z
/// where one exists.
///
/// The stored dataset is a LEFT join of the minified tractor sweeps onto the
/// photo-z catalog on `lsid`, so the astrometry is always present and only the
/// photo-z fields are optional. `ra_deg` is a hive partition directory rather
/// than a column, so it is not read here.
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct LsDr10PhotoZ {
    /// LS unique id: objid + (brickid << N) + (release << 40). Written
    /// unsigned upstream, but inside i64 range, which is what BSON stores.
    #[serde(rename(serialize = "_id"))]
    pub lsid: i64,
    pub ra: f64,
    pub dec: f64,
    pub ra_err: Option<f64>,
    pub dec_err: Option<f64>,
    pub z_phot: Option<f64>,
    pub z_phot_err: Option<f64>,
    pub photo_z_type: Option<String>,
}

impl super::arrow::FromRecordBatch for LsDr10PhotoZ {
    fn from_batch(
        batch: &::arrow::array::RecordBatch,
    ) -> Result<Vec<Self>, super::arrow::ColumnError> {
        use super::arrow::{f64_column, i64_column, string_column};

        let lsid = i64_column(batch, "lsid")?;
        let ra = f64_column(batch, "ra")?;
        let dec = f64_column(batch, "dec")?;
        let ra_err = f64_column(batch, "ra_err")?;
        let dec_err = f64_column(batch, "dec_err")?;
        let z_phot = f64_column(batch, "z_phot")?;
        let z_phot_err = f64_column(batch, "z_phot_err")?;
        let photo_z_type = string_column(batch, "photo_z_type")?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            // The tractor side of the join always has these; a row without them
            // is malformed rather than merely unmatched.
            let (Some(lsid), Some(ra), Some(dec)) = (lsid[row], ra[row], dec[row]) else {
                continue;
            };
            rows.push(LsDr10PhotoZ {
                lsid,
                ra,
                dec,
                ra_err: ra_err[row],
                dec_err: dec_err[row],
                z_phot: z_phot[row],
                z_phot_err: z_phot_err[row],
                photo_z_type: photo_z_type[row].clone().filter(|s| !s.is_empty()),
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for LsDr10PhotoZ {}

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
}
