apptainer instance list

SCRIPTS_DIR="$HOME/boom/apptainer/scripts"
$SCRIPTS_DIR/kafka-healthcheck.sh
$SCRIPTS_DIR/mongodb-healthcheck.sh
$SCRIPTS_DIR/valkey-healthcheck.sh