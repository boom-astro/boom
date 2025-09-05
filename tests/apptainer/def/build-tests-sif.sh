mkdir -p tests/apptainer/sif
apptainer build tests/apptainer/sif/mongo.sif tests/apptainer/def/mongo.def
apptainer build tests/apptainer/sif/valkey.sif tests/apptainer/def/valkey.def
apptainer build tests/apptainer/sif/kafka.sif tests/apptainer/def/kafka.def
apptainer build tests/apptainer/sif/boom-benchmarking.sif tests/apptainer/def/boom-benchmarking.def