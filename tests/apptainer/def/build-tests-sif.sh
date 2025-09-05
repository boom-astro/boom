mkdir -p tests/apptainer/sif
apptainer build tests/apptainer/sif/mongo.sif apptainer/def/mongo.def
apptainer build tests/apptainer/sif/valkey.sif apptainer/def/valkey.def
apptainer build tests/apptainer/sif/kafka.sif apptainer/def/kafka.def
apptainer build tests/apptainer/sif/boom-benchmarking.sif tests/apptainer/def/boom-benchmarking.def