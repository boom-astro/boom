# Alert processing

BOOM consumes Kafka streams of alerts from astronomical surveys
and outputs Kafka streams for consumers like SkyPortal.

Each alert is processed with the following pipeline:

1. Alerts are normalized to unify their schemas as much as possible.
   Their data is split and inserted into an alert dataset,
   an object dataset, and an image dataset, named according to the survey
   with which it is associated.
1. Cross-matches with object IDs from other data catalogs
   (from both live and archival surveys) are added.
   This is done based on the location (right ascension and declination)
   of the object in the alert.
1. Machine learning model classification scores are added.
1. A set of user-defined filters are applied.
   Any alert that passes through any filter is sent
   to a dedicated Kafka output stream for that alert's input stream.

The implementation is as follows:

```mermaid
graph LR
    subgraph Kafka
        ZTF[ZTF Kafka stream]
        LSST[LSST Kafka stream]
        Output1[Output Kafka stream 1]
        Output2[Output Kafka stream 2]
        OutputN[Output Kafka stream N]
    end

    subgraph Valkey
        ZTFAlertQueue[ZTF alert queue]
        LSSTAlertQueue[LSST alert queue]
    end

    subgraph BOOM services
        ZTFConsumer[ZTF Kafka consumer]
        LSSTConsumer[LSST Kafka consumer]
        subgraph Scheduler
            AlertWorker["Alert worker (database insertion and cross-matching)"]
            MLWorker[ML worker]
            FilterWorker[Filter worker]
        end
    end

    subgraph BOOM consumers
        SkyPortal[SkyPortal]
    end

    ZTF --> ZTFConsumer
    LSST --> LSSTConsumer
    ZTFConsumer --> ZTFAlertQueue
    LSSTConsumer --> LSSTAlertQueue
    ZTFAlertQueue --> AlertWorker
    LSSTAlertQueue --> AlertWorker
    AlertWorker --> ZTFAlertQueue
    AlertWorker --> LSSTAlertQueue
    ZTFAlertQueue --> MLWorker
    LSSTAlertQueue --> MLWorker
    MLWorker --> ZTFAlertQueue
    MLWorker --> LSSTAlertQueue
    ZTFAlertQueue --> FilterWorker
    LSSTAlertQueue --> FilterWorker
    FilterWorker -- Pass filter --> Output1
    FilterWorker -- Pass filter --> Output2
    FilterWorker -- Pass filter --> OutputN
    Output1 --> SkyPortal
    Output2 --> SkyPortal
    OutputN --> SkyPortal
```
