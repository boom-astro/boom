use serde::{Deserialize, Serialize};

#[derive(clap::ValueEnum, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Survey {
    Ztf,
    Lsst,
}

impl std::fmt::Display for Survey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Survey::Ztf => write!(f, "ZTF"),
            Survey::Lsst => write!(f, "LSST"),
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

impl std::fmt::Display for ProgramId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProgramId::Public => write!(f, "1"),
            ProgramId::Partnership => write!(f, "2"),
            ProgramId::Caltech => write!(f, "3"),
        }
    }
}
