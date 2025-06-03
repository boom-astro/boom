use serde::Serialize;

#[derive(clap::ValueEnum, Clone, Debug, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Survey {
    Ztf,
    Lsst,
}

impl Survey {
    pub fn as_str(&self) -> &str {
        match self {
            Survey::Ztf => "ZTF",
            Survey::Lsst => "LSST",
        }
    }
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ProgramId {
    #[default]
    #[serde(alias = "1")]
    Public = 1,
    #[serde(alias = "2")]
    Partnership = 2, // ZTF-only
    #[serde(alias = "3")]
    Caltech = 3, // ZTF-only
}

impl ProgramId {
    pub fn as_u8(&self) -> u8 {
        match self {
            ProgramId::Public => 1,
            ProgramId::Partnership => 2,
            ProgramId::Caltech => 3,
        }
    }
}
