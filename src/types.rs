use std::io::Read;

use apache_avro::from_value;
use apache_avro::{Reader, Schema, from_avro_datum};
use config::Value;
use flare::spatial::{radec2lb, deg2hms, deg2dms};
use mongodb::bson::doc;
use mongodb::bson::to_document;

pub fn ztf_alert_schema() -> Option<Schema> {
    // infer the schema from an avro file directly,
    // easier than merging the 5 schemas in schema/ztf
    let avro_file = std::fs::File::open("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
    let reader = match Reader::new(avro_file) {
        Ok(reader) => reader,
        Err(e) => {
            println!("Error creating avro reader: {}", e);
            return None;
        }
    };
    let schema = reader.writer_schema();
    Some(schema.to_owned())
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Cutout {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "stampData")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub stamp_data: Vec<u8>,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct PrvCandidate {
    pub jd: f64,
    pub fid: i32,
    pub pid: i64,
    #[serde(default = "default_prvcandidate_diffmaglim")]
    pub diffmaglim: Option<f32>,
    #[serde(default = "default_prvcandidate_pdiffimfilename")]
    pub pdiffimfilename: Option<String>,
    #[serde(default = "default_prvcandidate_programpi")]
    pub programpi: Option<String>,
    pub programid: i32,
    pub candid: Option<i64>,
    pub isdiffpos: Option<String>,
    #[serde(default = "default_prvcandidate_tblid")]
    pub tblid: Option<i64>,
    #[serde(default = "default_prvcandidate_nid")]
    pub nid: Option<i32>,
    #[serde(default = "default_prvcandidate_rcid")]
    pub rcid: Option<i32>,
    #[serde(default = "default_prvcandidate_field")]
    pub field: Option<i32>,
    #[serde(default = "default_prvcandidate_xpos")]
    pub xpos: Option<f32>,
    #[serde(default = "default_prvcandidate_ypos")]
    pub ypos: Option<f32>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    #[serde(default = "default_prvcandidate_chipsf")]
    pub chipsf: Option<f32>,
    #[serde(default = "default_prvcandidate_magap")]
    pub magap: Option<f32>,
    #[serde(default = "default_prvcandidate_sigmagap")]
    pub sigmagap: Option<f32>,
    #[serde(default = "default_prvcandidate_distnr")]
    pub distnr: Option<f32>,
    #[serde(default = "default_prvcandidate_magnr")]
    pub magnr: Option<f32>,
    #[serde(default = "default_prvcandidate_sigmagnr")]
    pub sigmagnr: Option<f32>,
    #[serde(default = "default_prvcandidate_chinr")]
    pub chinr: Option<f32>,
    #[serde(default = "default_prvcandidate_sharpnr")]
    pub sharpnr: Option<f32>,
    #[serde(default = "default_prvcandidate_sky")]
    pub sky: Option<f32>,
    #[serde(default = "default_prvcandidate_magdiff")]
    pub magdiff: Option<f32>,
    #[serde(default = "default_prvcandidate_fwhm")]
    pub fwhm: Option<f32>,
    #[serde(default = "default_prvcandidate_classtar")]
    pub classtar: Option<f32>,
    #[serde(default = "default_prvcandidate_mindtoedge")]
    pub mindtoedge: Option<f32>,
    #[serde(default = "default_prvcandidate_magfromlim")]
    pub magfromlim: Option<f32>,
    #[serde(default = "default_prvcandidate_seeratio")]
    pub seeratio: Option<f32>,
    #[serde(default = "default_prvcandidate_aimage")]
    pub aimage: Option<f32>,
    #[serde(default = "default_prvcandidate_bimage")]
    pub bimage: Option<f32>,
    #[serde(default = "default_prvcandidate_aimagerat")]
    pub aimagerat: Option<f32>,
    #[serde(default = "default_prvcandidate_bimagerat")]
    pub bimagerat: Option<f32>,
    #[serde(default = "default_prvcandidate_elong")]
    pub elong: Option<f32>,
    #[serde(default = "default_prvcandidate_nneg")]
    pub nneg: Option<i32>,
    #[serde(default = "default_prvcandidate_nbad")]
    pub nbad: Option<i32>,
    #[serde(default = "default_prvcandidate_rb")]
    pub rb: Option<f32>,
    #[serde(default = "default_prvcandidate_ssdistnr")]
    pub ssdistnr: Option<f32>,
    #[serde(default = "default_prvcandidate_ssmagnr")]
    pub ssmagnr: Option<f32>,
    #[serde(default = "default_prvcandidate_ssnamenr")]
    pub ssnamenr: Option<String>,
    #[serde(default = "default_prvcandidate_sumrat")]
    pub sumrat: Option<f32>,
    #[serde(default = "default_prvcandidate_magapbig")]
    pub magapbig: Option<f32>,
    #[serde(default = "default_prvcandidate_sigmagapbig")]
    pub sigmagapbig: Option<f32>,
    pub ranr: Option<f64>,
    pub decnr: Option<f64>,
    #[serde(default = "default_prvcandidate_scorr")]
    pub scorr: Option<f64>,
    #[serde(default = "default_prvcandidate_magzpsci")]
    pub magzpsci: Option<f32>,
    #[serde(default = "default_prvcandidate_magzpsciunc")]
    pub magzpsciunc: Option<f32>,
    #[serde(default = "default_prvcandidate_magzpscirms")]
    pub magzpscirms: Option<f32>,
    #[serde(default = "default_prvcandidate_clrcoeff")]
    pub clrcoeff: Option<f32>,
    #[serde(default = "default_prvcandidate_clrcounc")]
    pub clrcounc: Option<f32>,
    pub rbversion: String,
}

#[inline(always)]
fn default_prvcandidate_diffmaglim() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_pdiffimfilename() -> Option<String> { None }

#[inline(always)]
fn default_prvcandidate_programpi() -> Option<String> { None }

#[inline(always)]
fn default_prvcandidate_tblid() -> Option<i64> { None }

#[inline(always)]
fn default_prvcandidate_nid() -> Option<i32> { None }

#[inline(always)]
fn default_prvcandidate_rcid() -> Option<i32> { None }

#[inline(always)]
fn default_prvcandidate_field() -> Option<i32> { None }

#[inline(always)]
fn default_prvcandidate_xpos() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_ypos() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_chipsf() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magap() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_sigmagap() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_distnr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magnr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_sigmagnr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_chinr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_sharpnr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_sky() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magdiff() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_fwhm() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_classtar() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_mindtoedge() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magfromlim() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_seeratio() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_aimage() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_bimage() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_aimagerat() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_bimagerat() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_elong() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_nneg() -> Option<i32> { None }

#[inline(always)]
fn default_prvcandidate_nbad() -> Option<i32> { None }

#[inline(always)]
fn default_prvcandidate_rb() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_ssdistnr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_ssmagnr() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_ssnamenr() -> Option<String> { None }

#[inline(always)]
fn default_prvcandidate_sumrat() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magapbig() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_sigmagapbig() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_scorr() -> Option<f64> { None }

#[inline(always)]
fn default_prvcandidate_magzpsci() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magzpsciunc() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_magzpscirms() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_clrcoeff() -> Option<f32> { None }

#[inline(always)]
fn default_prvcandidate_clrcounc() -> Option<f32> { None }

/// avro alert schema
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FpHist {
    #[serde(default = "default_fphist_field")]
    pub field: Option<i32>,
    #[serde(default = "default_fphist_rcid")]
    pub rcid: Option<i32>,
    pub fid: i32,
    pub pid: i64,
    pub rfid: i64,
    #[serde(default = "default_fphist_sciinpseeing")]
    pub sciinpseeing: Option<f32>,
    #[serde(default = "default_fphist_scibckgnd")]
    pub scibckgnd: Option<f32>,
    #[serde(default = "default_fphist_scisigpix")]
    pub scisigpix: Option<f32>,
    #[serde(default = "default_fphist_magzpsci")]
    pub magzpsci: Option<f32>,
    #[serde(default = "default_fphist_magzpsciunc")]
    pub magzpsciunc: Option<f32>,
    #[serde(default = "default_fphist_magzpscirms")]
    pub magzpscirms: Option<f32>,
    #[serde(default = "default_fphist_clrcoeff")]
    pub clrcoeff: Option<f32>,
    #[serde(default = "default_fphist_clrcounc")]
    pub clrcounc: Option<f32>,
    #[serde(default = "default_fphist_exptime")]
    pub exptime: Option<f32>,
    #[serde(default = "default_fphist_adpctdif1")]
    pub adpctdif1: Option<f32>,
    #[serde(default = "default_fphist_adpctdif2")]
    pub adpctdif2: Option<f32>,
    #[serde(default = "default_fphist_diffmaglim")]
    pub diffmaglim: Option<f32>,
    pub programid: i32,
    pub jd: f64,
    #[serde(default = "default_fphist_forcediffimflux")]
    pub forcediffimflux: Option<f32>,
    #[serde(default = "default_fphist_forcediffimfluxunc")]
    pub forcediffimfluxunc: Option<f32>,
    #[serde(default = "default_fphist_procstatus")]
    pub procstatus: Option<String>,
    #[serde(default = "default_fphist_distnr")]
    pub distnr: Option<f32>,
    pub ranr: f64,
    pub decnr: f64,
    #[serde(default = "default_fphist_magnr")]
    pub magnr: Option<f32>,
    #[serde(default = "default_fphist_sigmagnr")]
    pub sigmagnr: Option<f32>,
    #[serde(default = "default_fphist_chinr")]
    pub chinr: Option<f32>,
    #[serde(default = "default_fphist_sharpnr")]
    pub sharpnr: Option<f32>,
}

#[inline(always)]
fn default_fphist_field() -> Option<i32> { None }

#[inline(always)]
fn default_fphist_rcid() -> Option<i32> { None }

#[inline(always)]
fn default_fphist_sciinpseeing() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_scibckgnd() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_scisigpix() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_magzpsci() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_magzpsciunc() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_magzpscirms() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_clrcoeff() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_clrcounc() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_exptime() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_adpctdif1() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_adpctdif2() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_diffmaglim() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_forcediffimflux() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_forcediffimfluxunc() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_procstatus() -> Option<String> { None }

#[inline(always)]
fn default_fphist_distnr() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_magnr() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_sigmagnr() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_chinr() -> Option<f32> { None }

#[inline(always)]
fn default_fphist_sharpnr() -> Option<f32> { None }

/// avro alert schema
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Candidate {
    pub jd: f64,
    pub fid: i32,
    pub pid: i64,
    #[serde(default = "default_candidate_diffmaglim")]
    pub diffmaglim: Option<f32>,
    #[serde(default = "default_candidate_pdiffimfilename")]
    pub pdiffimfilename: Option<String>,
    #[serde(default = "default_candidate_programpi")]
    pub programpi: Option<String>,
    pub programid: i32,
    pub candid: i64,
    pub isdiffpos: String,
    #[serde(default = "default_candidate_tblid")]
    pub tblid: Option<i64>,
    #[serde(default = "default_candidate_nid")]
    pub nid: Option<i32>,
    #[serde(default = "default_candidate_rcid")]
    pub rcid: Option<i32>,
    #[serde(default = "default_candidate_field")]
    pub field: Option<i32>,
    #[serde(default = "default_candidate_xpos")]
    pub xpos: Option<f32>,
    #[serde(default = "default_candidate_ypos")]
    pub ypos: Option<f32>,
    pub ra: f64,
    pub dec: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    #[serde(default = "default_candidate_chipsf")]
    pub chipsf: Option<f32>,
    #[serde(default = "default_candidate_magap")]
    pub magap: Option<f32>,
    #[serde(default = "default_candidate_sigmagap")]
    pub sigmagap: Option<f32>,
    #[serde(default = "default_candidate_distnr")]
    pub distnr: Option<f32>,
    #[serde(default = "default_candidate_magnr")]
    pub magnr: Option<f32>,
    #[serde(default = "default_candidate_sigmagnr")]
    pub sigmagnr: Option<f32>,
    #[serde(default = "default_candidate_chinr")]
    pub chinr: Option<f32>,
    #[serde(default = "default_candidate_sharpnr")]
    pub sharpnr: Option<f32>,
    #[serde(default = "default_candidate_sky")]
    pub sky: Option<f32>,
    #[serde(default = "default_candidate_magdiff")]
    pub magdiff: Option<f32>,
    #[serde(default = "default_candidate_fwhm")]
    pub fwhm: Option<f32>,
    #[serde(default = "default_candidate_classtar")]
    pub classtar: Option<f32>,
    #[serde(default = "default_candidate_mindtoedge")]
    pub mindtoedge: Option<f32>,
    #[serde(default = "default_candidate_magfromlim")]
    pub magfromlim: Option<f32>,
    #[serde(default = "default_candidate_seeratio")]
    pub seeratio: Option<f32>,
    #[serde(default = "default_candidate_aimage")]
    pub aimage: Option<f32>,
    #[serde(default = "default_candidate_bimage")]
    pub bimage: Option<f32>,
    #[serde(default = "default_candidate_aimagerat")]
    pub aimagerat: Option<f32>,
    #[serde(default = "default_candidate_bimagerat")]
    pub bimagerat: Option<f32>,
    #[serde(default = "default_candidate_elong")]
    pub elong: Option<f32>,
    #[serde(default = "default_candidate_nneg")]
    pub nneg: Option<i32>,
    #[serde(default = "default_candidate_nbad")]
    pub nbad: Option<i32>,
    #[serde(default = "default_candidate_rb")]
    pub rb: Option<f32>,
    #[serde(default = "default_candidate_ssdistnr")]
    pub ssdistnr: Option<f32>,
    #[serde(default = "default_candidate_ssmagnr")]
    pub ssmagnr: Option<f32>,
    #[serde(default = "default_candidate_ssnamenr")]
    pub ssnamenr: Option<String>,
    #[serde(default = "default_candidate_sumrat")]
    pub sumrat: Option<f32>,
    #[serde(default = "default_candidate_magapbig")]
    pub magapbig: Option<f32>,
    #[serde(default = "default_candidate_sigmagapbig")]
    pub sigmagapbig: Option<f32>,
    pub ranr: f64,
    pub decnr: f64,
    #[serde(default = "default_candidate_sgmag1")]
    pub sgmag1: Option<f32>,
    #[serde(default = "default_candidate_srmag1")]
    pub srmag1: Option<f32>,
    #[serde(default = "default_candidate_simag1")]
    pub simag1: Option<f32>,
    #[serde(default = "default_candidate_szmag1")]
    pub szmag1: Option<f32>,
    #[serde(default = "default_candidate_sgscore1")]
    pub sgscore1: Option<f32>,
    #[serde(default = "default_candidate_distpsnr1")]
    pub distpsnr1: Option<f32>,
    pub ndethist: i32,
    pub ncovhist: i32,
    #[serde(default = "default_candidate_jdstarthist")]
    pub jdstarthist: Option<f64>,
    #[serde(default = "default_candidate_jdendhist")]
    pub jdendhist: Option<f64>,
    #[serde(default = "default_candidate_scorr")]
    pub scorr: Option<f64>,
    #[serde(default = "default_candidate_tooflag")]
    pub tooflag: Option<i32>,
    #[serde(default = "default_candidate_objectidps1")]
    pub objectidps1: Option<i64>,
    #[serde(default = "default_candidate_objectidps2")]
    pub objectidps2: Option<i64>,
    #[serde(default = "default_candidate_sgmag2")]
    pub sgmag2: Option<f32>,
    #[serde(default = "default_candidate_srmag2")]
    pub srmag2: Option<f32>,
    #[serde(default = "default_candidate_simag2")]
    pub simag2: Option<f32>,
    #[serde(default = "default_candidate_szmag2")]
    pub szmag2: Option<f32>,
    #[serde(default = "default_candidate_sgscore2")]
    pub sgscore2: Option<f32>,
    #[serde(default = "default_candidate_distpsnr2")]
    pub distpsnr2: Option<f32>,
    #[serde(default = "default_candidate_objectidps3")]
    pub objectidps3: Option<i64>,
    #[serde(default = "default_candidate_sgmag3")]
    pub sgmag3: Option<f32>,
    #[serde(default = "default_candidate_srmag3")]
    pub srmag3: Option<f32>,
    #[serde(default = "default_candidate_simag3")]
    pub simag3: Option<f32>,
    #[serde(default = "default_candidate_szmag3")]
    pub szmag3: Option<f32>,
    #[serde(default = "default_candidate_sgscore3")]
    pub sgscore3: Option<f32>,
    #[serde(default = "default_candidate_distpsnr3")]
    pub distpsnr3: Option<f32>,
    pub nmtchps: i32,
    pub rfid: i64,
    pub jdstartref: f64,
    pub jdendref: f64,
    pub nframesref: i32,
    pub rbversion: String,
    #[serde(default = "default_candidate_dsnrms")]
    pub dsnrms: Option<f32>,
    #[serde(default = "default_candidate_ssnrms")]
    pub ssnrms: Option<f32>,
    #[serde(default = "default_candidate_dsdiff")]
    pub dsdiff: Option<f32>,
    #[serde(default = "default_candidate_magzpsci")]
    pub magzpsci: Option<f32>,
    #[serde(default = "default_candidate_magzpsciunc")]
    pub magzpsciunc: Option<f32>,
    #[serde(default = "default_candidate_magzpscirms")]
    pub magzpscirms: Option<f32>,
    pub nmatches: i32,
    #[serde(default = "default_candidate_clrcoeff")]
    pub clrcoeff: Option<f32>,
    #[serde(default = "default_candidate_clrcounc")]
    pub clrcounc: Option<f32>,
    #[serde(default = "default_candidate_zpclrcov")]
    pub zpclrcov: Option<f32>,
    #[serde(default = "default_candidate_zpmed")]
    pub zpmed: Option<f32>,
    #[serde(default = "default_candidate_clrmed")]
    pub clrmed: Option<f32>,
    #[serde(default = "default_candidate_clrrms")]
    pub clrrms: Option<f32>,
    #[serde(default = "default_candidate_neargaia")]
    pub neargaia: Option<f32>,
    #[serde(default = "default_candidate_neargaiabright")]
    pub neargaiabright: Option<f32>,
    #[serde(default = "default_candidate_maggaia")]
    pub maggaia: Option<f32>,
    #[serde(default = "default_candidate_maggaiabright")]
    pub maggaiabright: Option<f32>,
    #[serde(default = "default_candidate_exptime")]
    pub exptime: Option<f32>,
    #[serde(default = "default_candidate_drb")]
    pub drb: Option<f32>,
    pub drbversion: String,
}

#[inline(always)]
fn default_candidate_diffmaglim() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_pdiffimfilename() -> Option<String> { None }

#[inline(always)]
fn default_candidate_programpi() -> Option<String> { None }

#[inline(always)]
fn default_candidate_tblid() -> Option<i64> { None }

#[inline(always)]
fn default_candidate_nid() -> Option<i32> { None }

#[inline(always)]
fn default_candidate_rcid() -> Option<i32> { None }

#[inline(always)]
fn default_candidate_field() -> Option<i32> { None }

#[inline(always)]
fn default_candidate_xpos() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_ypos() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_chipsf() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magap() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sigmagap() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_distnr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magnr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sigmagnr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_chinr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sharpnr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sky() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magdiff() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_fwhm() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_classtar() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_mindtoedge() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magfromlim() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_seeratio() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_aimage() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_bimage() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_aimagerat() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_bimagerat() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_elong() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_nneg() -> Option<i32> { None }

#[inline(always)]
fn default_candidate_nbad() -> Option<i32> { None }

#[inline(always)]
fn default_candidate_rb() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_ssdistnr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_ssmagnr() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_ssnamenr() -> Option<String> { None }

#[inline(always)]
fn default_candidate_sumrat() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magapbig() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sigmagapbig() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sgmag1() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_srmag1() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_simag1() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_szmag1() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sgscore1() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_distpsnr1() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_jdstarthist() -> Option<f64> { None }

#[inline(always)]
fn default_candidate_jdendhist() -> Option<f64> { None }

#[inline(always)]
fn default_candidate_scorr() -> Option<f64> { None }

#[inline(always)]
fn default_candidate_tooflag() -> Option<i32> { None }

#[inline(always)]
fn default_candidate_objectidps1() -> Option<i64> { None }

#[inline(always)]
fn default_candidate_objectidps2() -> Option<i64> { None }

#[inline(always)]
fn default_candidate_sgmag2() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_srmag2() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_simag2() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_szmag2() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sgscore2() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_distpsnr2() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_objectidps3() -> Option<i64> { None }

#[inline(always)]
fn default_candidate_sgmag3() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_srmag3() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_simag3() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_szmag3() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_sgscore3() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_distpsnr3() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_dsnrms() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_ssnrms() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_dsdiff() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magzpsci() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magzpsciunc() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_magzpscirms() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_clrcoeff() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_clrcounc() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_zpclrcov() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_zpmed() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_clrmed() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_clrrms() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_neargaia() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_neargaiabright() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_maggaia() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_maggaiabright() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_exptime() -> Option<f32> { None }

#[inline(always)]
fn default_candidate_drb() -> Option<f32> { None }


#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Alert {
    pub schemavsn: String,
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    pub candidate: Candidate,
    #[serde(default = "default_alert_prv_candidates")]
    pub prv_candidates: Option<Vec<PrvCandidate>>,
    #[serde(default = "default_alert_fp_hists")]
    pub fp_hists: Option<Vec<FpHist>>,
    #[serde(default = "default_alert_cutout_science", rename = "cutoutScience")]
    pub cutout_science: Option<Cutout>,
    #[serde(default = "default_alert_cutout_template", rename = "cutoutTemplate")]
    pub cutout_template: Option<Cutout>,
    #[serde(default = "default_alert_cutout_difference", rename = "cutoutDifference")]
    pub cutout_difference: Option<Cutout>
}

#[inline(always)]
fn default_alert_prv_candidates() -> Option<Vec<PrvCandidate>> { None }

#[inline(always)]
fn default_alert_fp_hists() -> Option<Vec<FpHist>> { None }

#[inline(always)]
fn default_alert_cutout_science() -> Option<Cutout> { None }

#[inline(always)]
fn default_alert_cutout_template() -> Option<Cutout> { None }

#[inline(always)]
fn default_alert_cutout_difference() -> Option<Cutout> { None }

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct AlertNoHistory {
    pub schemavsn: String,
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    pub candidate: Candidate,
    #[serde(default = "default_alert_cutout_science", rename = "cutoutScience")]
    pub cutout_science: Option<Cutout>,
    #[serde(default = "default_alert_cutout_template", rename = "cutoutTemplate")]
    pub cutout_template: Option<Cutout>,
    #[serde(default = "default_alert_cutout_difference", rename = "cutoutDifference")]
    pub cutout_difference: Option<Cutout>
}

// make a function for the Alert type, that creates a AlertNoHistory type
// and returns a tuple with the AlertNoHistory, the prv_candidates and the fp_hist
impl Alert {
    pub fn pop_history(self) -> (AlertNoHistory, Option<Vec<PrvCandidate>>, Option<Vec<FpHist>>) {
        (
            AlertNoHistory {
                schemavsn: self.schemavsn,
                publisher: self.publisher,
                object_id: self.object_id,
                candid: self.candid,
                candidate: self.candidate,
                cutout_science: self.cutout_science,
                cutout_template: self.cutout_template,
                cutout_difference: self.cutout_difference,
            },
            self.prv_candidates,
            self.fp_hists,
        )
    }

    pub fn from_avro_bytes(avro_bytes: Vec<u8>) -> Result<Alert, Box<dyn std::error::Error>> {
        let reader = match Reader::new(&avro_bytes[..]) {
            Ok(reader) => reader,
            Err(e) => {
                println!("Error creating avro reader: {}", e);
                return Err(Box::new(e));
            }
        };
        
        let value = reader.map(|x| x.unwrap()).next().unwrap();
        let alert: Alert = from_value::<Alert>(&value).unwrap();
        Ok(alert)
    }

    pub fn from_avro_bytes_unsafe(avro_bytes: Vec<u8>, schema: &apache_avro::Schema) -> Result<Alert, Box<dyn std::error::Error>> {
        let mut cursor = std::io::Cursor::new(avro_bytes);

        let mut buf = [0; 4];
        cursor.read_exact(&mut buf)?; // avro identifier
        if buf != [b'O', b'b', b'j', 1u8] {
            return Err("Invalid Avro file".into());
        }

        let meta_schema = Schema::map(Schema::Bytes);
        from_avro_datum(&meta_schema, &mut cursor, None)?;

        // marker we don't need, should be equal to "gogenavromagic10" in bytes
        cursor.read_exact(&mut [0u8; 16])?;

        //  next 4 bytes contains info about the nb of messages and size
        // not needed since we only get one alert per avro packet
        cursor.read_exact(&mut [0u8; 4])?;

        // cursor.position() should be equal to 25641
        // we could technically hardcode this, but then even a subtle schema change might break it
        // at least here, we use from_avro_datum above to read the metadata and skip those bytes consistently

        let value = from_avro_datum(&schema, &mut cursor, None);
        match value {
            Ok(value) => {
                let alert: Alert = from_value::<Alert>(&value).unwrap();
                Ok(alert)
            }
            Err(e) => {
                println!("Error deserializing avro message: {}", e);
                Err(Box::new(e))
            }
        }
    }

}

impl AlertNoHistory {
    // add a function to convert the AlertNoHistory to a mongodb document
    pub fn mongify(self) -> mongodb::bson::Document {
        let mut doc = to_document(&self).unwrap();
        let (l, b) = radec2lb(self.candidate.ra, self.candidate.dec);
        let coordinates = doc! {
            "radec_geojson": {
                "type": "Point",
                "coordinates": [self.candidate.ra - 180.0, self.candidate.dec]
            },
            "radec_str": [deg2hms(self.candidate.ra), deg2dms(self.candidate.dec)],
            "l": l,
            "b": b
        };
        doc.insert("coordinates", coordinates);
        doc
    }
}

impl PrvCandidate {
    pub fn mongify(self) -> mongodb::bson::Document {
        // sanitize it by removing fields with None values
        let mut cleaned_doc = mongodb::bson::Document::new();
        for (key, value) in to_document(&self).unwrap() {
            if value != mongodb::bson::Bson::Null {
                cleaned_doc.insert(key, value);
            }
        }
        cleaned_doc
    }
}

// same for the fp_hists
impl FpHist {
    pub fn mongify(self) -> mongodb::bson::Document {
        // sanitize it by removing fields with None values
        let mut cleaned_doc = mongodb::bson::Document::new();
        for (key, value) in to_document(&self).unwrap() {
            if value != mongodb::bson::Bson::Null {
                cleaned_doc.insert(key, value);
            }
        }
        cleaned_doc
    }
}

#[derive(Debug)]    
pub struct CatalogXmatchConfig {
    pub catalog: String, // name of the collection in the database
    pub radius: f64, // radius in radians
    pub projection: mongodb::bson::Document, // projection to apply to the catalog
    pub use_distance: bool, // whether to use the distance field in the crossmatch
    pub distance_key: Option<String>, // name of the field to use for distance
    pub distance_max: Option<f64>, // maximum distance in kpc
    pub distance_max_near: Option<f64>, // maximum distance in arcsec for nearby objects
}

impl CatalogXmatchConfig {
    pub fn new(
        catalog: &str,
        radius: f64,
        projection: mongodb::bson::Document,
        use_distance: bool,
        distance_key: Option<String>,
        distance_max: Option<f64>,
        distance_max_near: Option<f64>
    ) -> CatalogXmatchConfig {
        CatalogXmatchConfig {
            catalog: catalog.to_string(),
            radius: radius * std::f64::consts::PI / 180.0 / 3600.0, // convert arcsec to radians
            projection,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near
        }
    }

    // based on the code in the main function, create a from_config function
    pub fn from_config(config_value: Value) -> CatalogXmatchConfig {
        let hashmap_xmatch = config_value.into_table().unwrap();

        // any of the fields can be missing, so we need to carefully handle the Option type
        let catalog = {
            if let Some(catalog) = hashmap_xmatch.get("catalog") {
                catalog.clone().into_string().unwrap()
            } else {
                // raise an error
                panic!("catalog field is missing");
            }
        };

        let radius = {
            if let Some(radius) = hashmap_xmatch.get("radius") {
                radius.clone().into_float().unwrap()
            } else {
                panic!("radius field is missing");
            }
        };

        let projection = {
            if let Some(projection) = hashmap_xmatch.get("projection") {
                projection.clone().into_table().unwrap()
            } else {
                panic!("projection field is missing");
            }
        };

        let use_distance = {
            if let Some(use_distance) = hashmap_xmatch.get("use_distance") {
                use_distance.clone().into_bool().unwrap()
            } else {
                false
            }
        };

        let distance_key = {
            if let Some(distance_key) = hashmap_xmatch.get("distance_key") {
                Some(distance_key.clone().into_string().unwrap())
            } else {
                None
            }
        };

        let distance_max = {
            if let Some(distance_max) = hashmap_xmatch.get("distance_max") {
                Some(distance_max.clone().into_float().unwrap())
            } else {
                None
            }
        };

        let distance_max_near = {
            if let Some(distance_max_near) = hashmap_xmatch.get("distance_max_near") {
                Some(distance_max_near.clone().into_float().unwrap())
            } else {
                None
            }
        };

        // projection is a hashmap, we need to convert it to a Document
        let mut projection_doc = mongodb::bson::Document::new();
        for (key, value) in projection.iter() {
            let key = key.as_str();
            let value = value.clone().into_int().unwrap();
            projection_doc.insert(key, value);
        }

        if use_distance {
            if distance_key.is_none() {
                panic!("must provide a distance_key if use_distance is true");
            }

            if distance_max.is_none() {
                panic!("must provide a distance_max if use_distance is true");
            }

            if distance_max_near.is_none() {
                panic!("must provide a distance_max_near if use_distance is true");
            }
        }

        CatalogXmatchConfig::new(
            &catalog,
            radius,
            projection_doc,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near
        )
    }
}
