mkdir -p apptainer/sif
apptainer build apptainer/sif/mongo.sif apptainer/def/mongo.def
apptainer build apptainer/sif/valkey.sif apptainer/def/valkey.def
apptainer build apptainer/sif/kafka.sif apptainer/def/kafka.def
apptainer build apptainer/sif/boom.sif apptainer/def/boom.def
apptainer build apptainer/sif/boom-benchmarking.sif apptainer/def/boom-benchmarking.def