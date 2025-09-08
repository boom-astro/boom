mkdir -p apptainer/sif
apptainer build apptainer/sif/mongo.sif apptainer/def/mongo.def
apptainer build apptainer/sif/valkey.sif apptainer/def/valkey.def
apptainer build apptainer/sif/kafka.sif apptainer/def/kafka.def
apptainer build apptainer/sif/boom.sif apptainer/def/boom.def
apptainer build apptainer/sif/otel-collector.sif docker://otel/opentelemetry-collector:0.131.1
apptainer build apptainer/sif/prometheus.sif docker://prom/prometheus:latest
apptainer build apptainer/sif/uptime-kuma.sif apptainer/def/uptime-kuma.def