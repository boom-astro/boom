apptainer build apptainer/sif/mongo.sif apptainer/def/mongo.def
apptainer build apptainer/sif/valkey.sif apptainer/def/valkey.def
apptainer build apptainer/sif/kafka.sif apptainer/def/kafka.def
apptainer build apptainer/sif/producer.sif docker-daemon://boom/boom-benchmarking:latest
apptainer build apptainer/sif/consumer.sif docker-daemon://boom/boom-benchmarking:latest
apptainer build apptainer/sif/scheduler.sif docker-daemon://boom/boom-benchmarking:latest