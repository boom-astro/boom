#!/bin/bash
# Arguments:
#   test - send message to testing chat instead of info chat

BOOM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Retrieves the boom directory
HEALTHCHECK_DIR="$BOOM_DIR/apptainer/scripts/healthcheck"
KAFKA_TEMP_GROUP="temp_group_$(date +%s)"

RED="\e[31m"
END="\e[0m"

get_kafka_temp_group(){
  local survey=$1
  echo "${KAFKA_TEMP_GROUP}_${survey}"
}

# --- Consume one message from each survey to create the temp group ---
for survey in LSST ZTF; do
  apptainer exec instance://kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${survey}_alerts_results" \
    --group "$(get_kafka_temp_group "$survey")" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 > /dev/null 2>&1
done

if [[ -f "$BOOM_DIR/.env" ]]; then
  set -a
  source "$BOOM_DIR/.env"
  set +a
else
  echo -e "${RED}Error: .env file is missing in $BOOM_DIR${END}"
  exit 1
fi

current_datetime() {
  TZ=utc date "+%Y-%m-%d"
}

get_count_file(){
  local survey=$1
  if [[ "$survey" == "LSST" ]]; then
    echo "$BOOM_DIR/apptainer/persistent/lsst_count.txt"
  else
    echo "$BOOM_DIR/apptainer/persistent/ztf_count.txt"
  fi
}

# --- Function to get count from MongoDB ---
get_mongo_count(){
  local collection=$1
  apptainer exec instance://mongo \
    mongosh "mongodb://$BOOM_DATABASE__USERNAME:$BOOM_DATABASE__PASSWORD@localhost:27017/boom?authSource=admin" \
    --quiet \
    --eval "db.$collection.estimatedDocumentCount()"
}

get_kafka_count(){
  local survey=$1
  apptainer exec instance://kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group "$(get_kafka_temp_group "$survey")" \
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
  local survey=$4
  echo "$(current_datetime) - $survey:

New alerts : $(format "$((db["${survey}_alerts"] - old_alerts_count))")
Filtered       : $filtered_msg

-      -      MongoDB      -      -
Alerts    :   $(format "${db["${survey}_alerts"]}")
Aux        :   $(format "${db["${survey}_alerts_aux"]}")
Cutouts :   $(format "${db["${survey}_alerts_cutouts"]}")"
}

collections=("" "_aux" "_cutouts")

separator="
------------------------
"

if [[ "$1" == "test" ]]; then
  chat_id=$BOOM_TESTING_CHAT_ID
else
  chat_id=$BOOM_CHAT_ID
fi

declare -A old_alerts_count=(
  [LSST]=0
  [LSST_filtered]=0
  [ZTF]=0
  [ZTF_filtered]=0
)

# --- Read old LSST_alerts count if file exists (formatted as: all_alerts:filtered_alerts) ---
if [[ -f "$(get_count_file LSST)" ]]; then
    read -r alerts filtered_alerts < <(tr ':' ' ' < "$(get_count_file LSST)")
    old_alerts_count[LSST]=$alerts
    old_alerts_count[LSST_filtered]=$filtered_alerts
fi

# --- Read old ZTF_alerts count if file exists (formatted as: all_alerts:filtered_alerts) ---
if [[ -f "$(get_count_file ZTF)" ]]; then
    read -r alerts filtered_alerts < <(tr ':' ' ' < "$(get_count_file ZTF)")
    old_alerts_count[ZTF]=$alerts
    old_alerts_count[ZTF_filtered]=$filtered_alerts
fi

declare -A current_db
while true; do
  message_to_send=""

  # Process each survey
  for survey in LSST ZTF; do
    # --- Get new counts from Kafka ---
    filtered_alerts=0
    if "$HEALTHCHECK_DIR/kafka-healthcheck.sh" > /dev/null 2>&1; then
      filtered_alerts=$(get_kafka_count "$survey")
      filtered_msg=$(format "$((filtered_alerts - old_alerts_count["${survey}_filtered"]))")
      old_alerts_count["${survey}_filtered"]=$filtered_alerts
    else
      filtered_alerts=${old_alerts_count["${survey}_filtered"]}
      filtered_msg="Kafka is down!"
    fi

    # --- Get new counts from MongoDB ---
    if "$HEALTHCHECK_DIR/mongodb-healthcheck.sh" > /dev/null 2>&1; then
      for col in "${collections[@]}"; do
          current_db["${survey}_alerts${col}"]=$(get_mongo_count "${survey}_alerts${col}")
      done
      # --- Prepare message ---
      message="$(prepare_message current_db "${old_alerts_count["${survey}"]}" "$filtered_msg" "$survey")"
      if [[ "$message_to_send" == "" ]]; then
        message_to_send+="$message"
      else # Add a separator between survey messages
        message_to_send+="$separator$message"
      fi
      old_alerts_count["${survey}"]=${current_db["${survey}_alerts"]}
    else
      current_db["${survey}_alerts"]=${old_alerts_count["${survey}"]}
      message_to_send="$(current_datetime) MongoDB is down!"
    fi

    # --- Save new filtered alerts count to file ---
    echo "${current_db["${survey}_alerts"]}:$filtered_alerts" > "$(get_count_file "$survey")"

  done

  # --- Send message to Telegram ---
  curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
       --data-urlencode "chat_id=$chat_id" \
       --data-urlencode "text=$message_to_send" > /dev/null

  # --- Wait until next 08:00:00 AM America/Los_Angeles time (ZTF Time Zone) ---
  sleep $((($(TZ="America/Los_Angeles" date -d "08:00:00" +%s) - $(TZ="America/Los_Angeles" date +%s) + 86400) % 86400 ))
done