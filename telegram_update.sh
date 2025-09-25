#!/bin/bash
# Arguments:
#   test - send message to testing chat instead of info chat

BOOM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Retrieves the boom directory
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
COUNT_FILE="$BOOM_DIR/apptainer/persistent/count.txt"
KAFKA_GROUP="temp_group"

if [[ -f "$BOOM_DIR/.env" ]]; then
  set -a
  source "$BOOM_DIR/.env"
  set +a
else
  echo -e "\e[31mError: .env file is missing in $BOOM_DIR\e[0m"
  exit 1
fi

current_datetime() {
  TZ=utc date "+%Y-%m-%d"
}

# --- Function to get count from MongoDB ---
get_mongo_count(){
  local collection=$1
  apptainer exec instance://mongo \
    mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom?authSource=admin" \
    --quiet \
    --eval "db.$collection.countDocuments()"
}

get_kafka_count(){
  apptainer exec instance://broker kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group "$KAFKA_GROUP" \
  --describe 2>/dev/null \
  | awk 'NR>1 {sum += $5} END {print sum}'
}

# --- Function to format numbers with spaces ---
format(){
  local num=$1
  echo "$num" | rev | sed 's/\([0-9]\{3\}\)/\1 /g' | rev | sed 's/^ //'
}

prepare_message(){
  local -n db=$1
  local old_alerts_count=$2
  local filtered_msg=$3
  message_to_send="$(current_datetime) - LSST:

New alerts : $(format "$((db["LSST_alerts"] - old_alerts_count))")
Filtered       : $filtered_msg

 -      -      MongoDB      -      -
Alerts    :   $(format "${db["LSST_alerts"]}")
Aux        :   $(format "${db["LSST_alerts_aux"]}")
Cutouts :   $(format "${db["LSST_alerts_cutouts"]}")"
}

collections=("LSST_alerts" "LSST_alerts_aux" "LSST_alerts_cutouts")
message_to_send=""
if [[ "$1" == "test" ]]; then
  chat_id=$BOOM_TESTING_CHAT_ID
else
  chat_id=$BOOM_CHAT_ID
fi

old_alerts_count=0
old_filtered_alerts_count=0
# --- Read old LSST_alerts count if file exists (formatted as: all_alerts:filtered_alerts) ---
if [[ -f "$COUNT_FILE" ]]; then
    read -r old_alerts_count old_filtered_alerts_count < <(tr ':' ' ' < "$COUNT_FILE")
fi

while true; do
  # --- Get new counts from Kafka ---
  filtered_alerts=0
  if "$SCRIPTS_DIR/kafka-healthcheck.sh" > /dev/null 2>&1; then
    filtered_alerts=$(get_kafka_count)
    filtered_msg=$(format "$((filtered_alerts - old_filtered_alerts_count))")
  else
    filtered_alerts=old_filtered_alerts_count
    filtered_msg="Kafka is down!"
  fi

  # --- Get new counts from MongoDB ---
  declare -A current_db
  if "$SCRIPTS_DIR/mongodb-healthcheck.sh" > /dev/null 2>&1; then
    for col in "${collections[@]}"; do
        current_db["$col"]=$(get_mongo_count "$col")
    done
    # --- Prepare message ---
    prepare_message current_db "$old_alerts_count" "$filtered_msg"
    old_alerts_count=${current_db["LSST_alerts"]}
  else
    current_db["LSST_alerts"]=$old_alerts_count
    message_to_send="$(current_datetime) MongoDB is down!"
  fi

  # --- Save new filtered alerts count to file ---
  echo "${current_db["LSST_alerts"]}:$filtered_alerts" > "$COUNT_FILE"

  # --- Send message to Telegram ---
  curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
       --data-urlencode "chat_id=$chat_id" \
       --data-urlencode "text=$message_to_send" > /dev/null

  # --- Wait until next 09:00:00 AM America/Santiago time (LSST time zone) ---
  sleep $((($(TZ="America/Santiago" date -d "09:00:00" +%s) - $(TZ="America/Santiago" date +%s) + 86400) % 86400 ))
done