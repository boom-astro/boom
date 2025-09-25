#!/usr/bin/env bash

RED="\e[31m"
GREEN="\e[0;32m"
END="\e[0m"

exec_db_cmd_in_mongo() {
  local cmd="$1"
  apptainer exec instance://mongo \
    mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom?authSource=admin" \
    --eval "$cmd"
}

file="$1"

if [ -z "$file" ]; then
  echo -e "${RED}No file provided. Usage: add_filters.sh <path_to_filter_json>${END}"
  exit 1
fi

if [ ! -f "$file" ]; then
  echo -e "${RED}File not found: $file${END}"
  exit 1
fi

result=$(exec_db_cmd_in_mongo "db.createCollection('filters')")
if echo "$result" | grep -q 'ok: 1'; then
  echo -e "${GREEN}Collection 'filters' created successfully or already exists.${END}"
else
  echo -e "${RED}Failed to create collection 'filters'.${END}"
  exit 1
fi
cpt=1
jq -c '.[]' "$file" | while read -r filter; do
  result=$(exec_db_cmd_in_mongo "db.filters.insertOne($filter)")
  if echo "$result" | grep -q 'acknowledged: true'; then
    echo -e "${GREEN}Filter ${cpt} added successfully.${END}"
  else
    echo -e "${RED}Failed to add filter ${cpt}.${END}"
  fi
  ((cpt++))
done