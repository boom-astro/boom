use consumer::{
    App, Config, ConsumerConfig, ConsumerTask, Source,
    sources::{
        decam::{DecamArgs, DecamSource},
        lsst::{LsstArgs, LsstSource},
        ztf::{ZtfArgs, ZtfSource},
    },
};

use std::path::PathBuf;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use clap::{Parser, Subcommand};
use color_eyre::eyre::{Report, WrapErr, eyre};

/// A parsable type that can be converted into `rdkafka::Offset`.
#[derive(Clone, Debug, PartialEq)]
enum Offset {
    Beginning,
    End,
    DateTime(DateTime<Utc>),
    Stored,
}

impl From<Offset> for rdkafka::Offset {
    fn from(value: Offset) -> Self {
        match value {
            Offset::Beginning => rdkafka::Offset::Beginning,
            Offset::End => rdkafka::Offset::End,
            Offset::DateTime(datetime) => rdkafka::Offset::Offset(datetime.timestamp()),
            Offset::Stored => rdkafka::Offset::Stored,
        }
    }
}

impl std::str::FromStr for Offset {
    type Err = Report;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "beginning" => Ok(Self::Beginning),
            "end" => Ok(Self::End),
            "stored" => Ok(Self::Stored),
            _ if s.starts_with("@") => {
                let value = s.strip_prefix("@").unwrap();
                let datetime = Ok(value)
                    //
                    // First, try as YYYYMMDD (there's no ambiguity between date
                    // ints and timestamps because any practical timestamp will
                    // have ten digits, not eight):
                    .and_then(|value| {
                        NaiveDate::parse_from_str(value, "%Y%m%d")
                            .map(|date| NaiveDateTime::from(date).and_utc())
                    })
                    //
                    // Next, try as YYYY-MM-DD:
                    .or_else(|_| {
                        NaiveDate::parse_from_str(value, "%Y-%m-%d")
                            .map(|date| NaiveDateTime::from(date).and_utc())
                    })
                    //
                    // Next, try as RFC 3339:
                    .or_else(|_| {
                        DateTime::parse_from_rfc3339(value).map(|datetime| datetime.to_utc())
                    })
                    //
                    // Finally, try as a Unix timestamp:
                    .or_else(|_| {
                        value
                            .parse::<i64>()
                            .wrap_err("invalid unix timestamp")
                            .and_then(|timestamp| {
                                DateTime::from_timestamp(timestamp, 0).ok_or_else(|| {
                                    eyre!("invalid unix timestamp: {timestamp} is out of range")
                                })
                            })
                    })?;
                Ok(Self::DateTime(datetime))
            }
            _ => Err(eyre!("invalid OFFSET value")),
        }
    }
}

/// Consume messages from kafka and enqueue them in redis
#[derive(Parser)]
struct Cli {
    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: PathBuf,

    /// Number of consumers to use to read the kafka stream
    #[arg(long, default_value_t = 1)]
    consumers: usize,

    /// Start consuming at a given offset [possible values: beginning, end,
    /// stored, @YYYYMMDD, @YYYY-MM-DD, @<RFC 3339>, @<Unix timestamp>]
    ///
    /// beginning: Start at the first message in the topic.
    ///
    /// end: Start at the end of the topic, only consume new messages.
    ///
    /// stored: Start from the last stored offset or the beginning.
    ///
    /// @YYYYMMDD | @YYYY-MM-DD | @<RFC 3339> | @<timestamp>: Start at the
    /// offset corresponding to the timestamp derived from the given value,
    /// which may be a date in YYYYMMDD or YYYY-MM-DD format, any valid RFC 3339
    /// string (e.g., 2011-09-25T00:00:00Z), or an integer representing a Unix
    /// timestamp (e.g., 1316926800).
    #[arg(long, value_name = "OFFSET", value_parser = str::parse::<Offset>, default_value = "stored")]
    from: Offset,

    /// Clear the redis queue before consuming from kafka
    #[arg(long)]
    clear: bool,

    #[command(subcommand)]
    source: SourceCommand,
}

#[derive(Subcommand)]
enum SourceCommand {
    /// Consume alerts from DECam
    Decam(DecamArgs),

    /// Consume alerts from LSST
    Lsst(LsstArgs),

    /// Consume alerts from ZTF
    Ztf(ZtfArgs),
}

impl SourceCommand {
    fn build(&self, config: &ConsumerConfig) -> Box<dyn Source + Send + Sync + 'static> {
        match self {
            Self::Decam(args) => Box::new(DecamSource::from((args, &config.decam))),
            Self::Lsst(args) => Box::new(LsstSource::from((args, &config.lsst))),
            Self::Ztf(args) => Box::new(ZtfSource::from((args, &config.ztf))),
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = Config::load(&cli.config).expect("failed to load config");
    let consumer_task = ConsumerTask::new(
        config.consumer.app.instance_id,
        cli.source.build(&config.consumer),
        cli.from.into(),
        config.consumer.task,
    );

    App::new(
        cli.clear,
        cli.consumers,
        consumer_task,
        config.consumer.app,
        config.boom.redis_uri(),
    )
    .run()
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::LazyLock;

    use chrono::{NaiveTime, TimeZone};

    static DATETIME: LazyLock<DateTime<Utc>> =
        LazyLock::new(|| Utc.with_ymd_and_hms(2011, 9, 25, 5, 0, 0).unwrap());

    #[test]
    fn offset_parse_beginning() {
        assert_eq!("beginning".parse::<Offset>().unwrap(), Offset::Beginning);
    }

    #[test]
    fn offset_parse_end() {
        assert_eq!("end".parse::<Offset>().unwrap(), Offset::End);
    }

    #[test]
    fn offset_parse_stored() {
        assert_eq!("stored".parse::<Offset>().unwrap(), Offset::Stored);
    }

    #[test]
    fn offset_parse_date_int() {
        assert_eq!(
            "@20110925".parse::<Offset>().unwrap(),
            Offset::DateTime(DATETIME.with_time(NaiveTime::MIN).unwrap())
        );
    }

    #[test]
    fn offset_parse_date() {
        assert_eq!(
            "@2011-09-25".parse::<Offset>().unwrap(),
            Offset::DateTime(DATETIME.with_time(NaiveTime::MIN).unwrap())
        );
    }

    #[test]
    fn offset_parse_rfc3339() {
        assert_eq!(
            "@2011-09-25T05:00:00Z".parse::<Offset>().unwrap(),
            Offset::DateTime(*DATETIME)
        );
    }

    #[test]
    fn offset_parse_timestamp() {
        assert_eq!(
            "@1316926800".parse::<Offset>().unwrap(),
            Offset::DateTime(*DATETIME)
        );
    }
}
