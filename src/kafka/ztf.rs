use crate::{
    kafka::base::{AlertConsumer, AlertProducer},
    utils::{
        data::{count_files_in_dir, download_to_file},
        enums::{ProgramId, Survey},
    },
};
use tempfile::NamedTempFile;
use tracing::{info, instrument};

const ZTF_DEFAULT_NB_PARTITIONS: usize = 15;

pub struct ZtfAlertConsumer {
    output_queue: String,
    program_id: ProgramId,
}

impl ZtfAlertConsumer {
    #[instrument]
    pub fn new(output_queue: Option<&str>, program_id: Option<ProgramId>) -> Self {
        let program_id = program_id.unwrap_or(ProgramId::Public);
        let output_queue = output_queue
            .unwrap_or("ZTF_alerts_packets_queue")
            .to_string();

        ZtfAlertConsumer {
            output_queue,
            program_id,
        }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for ZtfAlertConsumer {
    fn topic_name(&self, timestamp: i64) -> String {
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        format!("ztf_{}_programid{}", date.format("%Y%m%d"), self.program_id)
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> Survey {
        Survey::Ztf
    }
}

pub struct ZtfAlertProducer {
    date: chrono::NaiveDate,
    program_id: ProgramId,
    limit: i64,
    partnership_archive_username: Option<String>,
    partnership_archive_password: Option<String>,
    server_url: String,
    verbose: bool,
    working_dir: String,
}

impl ZtfAlertProducer {
    pub fn new(
        date: chrono::NaiveDate,
        limit: i64,
        program_id: ProgramId,
        server_url: &str,
        verbose: bool,
    ) -> Self {
        // if program_id > 1, check that we have a ZTF_PARTNERSHIP_ARCHIVE_USERNAME
        // and ZTF_PARTNERSHIP_ARCHIVE_PASSWORD set as env variables
        let partnership_archive_username = match std::env::var("ZTF_PARTNERSHIP_ARCHIVE_USERNAME") {
            Ok(username) => Some(username),
            Err(_) => None,
        };
        let partnership_archive_password = match std::env::var("ZTF_PARTNERSHIP_ARCHIVE_PASSWORD") {
            Ok(password) => Some(password),
            Err(_) => None,
        };
        if program_id == ProgramId::Partnership
            && (partnership_archive_username.is_none() || partnership_archive_password.is_none())
        {
            panic!("ZTF_PARTNERSHIP_ARCHIVE_USERNAME and ZTF_PARTNERSHIP_ARCHIVE_PASSWORD environment variables must be set for partnership program ID");
        }

        ZtfAlertProducer {
            date,
            limit,
            program_id,
            partnership_archive_username,
            partnership_archive_password,
            server_url: server_url.to_string(),
            verbose,
            working_dir: ".".to_string(),
        }
    }

    pub fn with_working_dir(self, working_dir: &str) -> Self {
        Self {
            working_dir: working_dir.to_string(),
            ..self
        }
    }
}

#[async_trait::async_trait]
impl AlertProducer for ZtfAlertProducer {
    fn topic_name(&self) -> String {
        format!(
            "ztf_{}_programid{}",
            self.date.format("%Y%m%d"),
            self.program_id
        )
    }
    fn data_directory(&self) -> String {
        let program = match self.program_id {
            ProgramId::Public => "public",
            ProgramId::Partnership => "partnership",
            ProgramId::Caltech => "caltech",
        };
        format!(
            "{}/data/alerts/ztf/{}/{}",
            self.working_dir,
            program,
            self.date.format("%Y%m%d")
        )
    }
    fn server_url(&self) -> String {
        self.server_url.clone()
    }
    fn limit(&self) -> i64 {
        self.limit
    }
    fn verbose(&self) -> bool {
        self.verbose
    }
    fn default_nb_partitions(&self) -> usize {
        ZTF_DEFAULT_NB_PARTITIONS
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        let date_str = self.date.format("%Y%m%d").to_string();
        info!(
            "Downloading alerts for date {} (programid: {:?})",
            date_str, self.program_id
        );

        let (file_name, base_url) = match self.program_id {
            ProgramId::Public => (
                format!("ztf_public_{}.tar.gz", date_str),
                "https://ztf.uw.edu/alerts/public/".to_string(),
            ),
            ProgramId::Partnership => (
                format!("ztf_partnership_{}.tar.gz", date_str),
                "https://ztf.uw.edu/alerts/partnership/".to_string(),
            ),
            _ => return Err("Unsupported program ID for ZTF alerts".into()),
        };

        let data_folder = self.data_directory();
        std::fs::create_dir_all(&data_folder)?;

        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
        if count > 0 {
            info!("Alerts already downloaded to {}", data_folder);
            return Ok(count as i64);
        }

        let mut output_temp_file = NamedTempFile::new_in(&data_folder)
            .map_err(|e| format!("Failed to create temp file: {}", e))?;

        // use download_to_file function to download the file
        match download_to_file(
            &mut output_temp_file,
            &format!("{}{}", base_url, file_name),
            self.partnership_archive_username.as_deref(),
            self.partnership_archive_password.as_deref(),
            self.verbose,
        )
        .await
        {
            Ok(_) => info!("Downloaded alerts to {}", data_folder),
            Err(e) => {
                if e.to_string().contains("404 Not Found") {
                    return Err("No alerts found for this date".into());
                } else {
                    return Err(e);
                }
            }
        }

        let output_temp_path = output_temp_file.path().to_str().unwrap();

        // when we untar it, the name of the folder should be the same as the file name
        let output = std::process::Command::new("tar")
            .arg("-xzf")
            .arg(output_temp_path)
            .arg("-C")
            .arg(&data_folder)
            .output()?;
        if !output.status.success() {
            return Err("Failed to extract alerts".into());
        } else {
            info!("Extracted alerts to {}", data_folder);
        }

        drop(output_temp_file); // Close the temp file

        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;

        Ok(count as i64)
    }
}
