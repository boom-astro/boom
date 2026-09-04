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
