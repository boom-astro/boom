#!/usr/bin/env bash

# Only import NED alerts if the collection does not exist
NED_COLLECTION_NAME="NED"
NED_COLLECTION_EXISTS=$(mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" --quiet --eval "db.getCollectionNames().includes('$NED_COLLECTION_NAME')")
echo "NED collection exists: $NED_COLLECTION_EXISTS"

if [ "$NED_COLLECTION_EXISTS" = "false" ]; then
    echo "Creating collection $NED_COLLECTION_NAME"
    mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" \
        --eval "db.createCollection('$NED_COLLECTION_NAME')"

    echo "Creating 2d index on coordinates.radec_geojson for $NED_COLLECTION_NAME"
    mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" \
        --eval "db.$NED_COLLECTION_NAME.createIndex({ 'coordinates.radec_geojson': '2dsphere' })"

    echo "Importing NED alerts into $DB_NAME MongoDB database"
    gunzip -kc /kowalski.NED.json.gz | \
        mongoimport \
        "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin$DB_ADD_URI" \
        --collection $NED_COLLECTION_NAME \
        --jsonArray
else
    echo "NED alerts already imported; skipping import"
fi

# Always drop ZTF_alerts, ZTF_alerts_aux, ZTF_alerts_cutouts, and filters collections
mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" --quiet --eval "
    db.ZTF_alerts.drop();
    db.ZTF_alerts_aux.drop();
    db.ZTF_alerts_cutouts.drop();
    db.filters.drop();"

# add the filters table with an index on filter_id
mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" \
    --eval "db.createCollection('filters'); db.filters.createIndex({ filter_id: 1 })"

N_FILTERS=25

# have a function to randomly generate a UUID
generate_uuid() {
    cat /proc/sys/kernel/random/uuid
}

# ingest N_FILTERS copies of the cats150 filter into filters collection
echo "Ingesting $N_FILTERS copies of the cats150 filter into filters collection"
for i in $(seq 1 $N_FILTERS); do
    echo "Inserting cats150 filter with filter_id $i into filters collection"
    # the file contains one document, so we read and edit the filter _id field
    EDITED_FILTER_CONTENT=$(jq --arg id "$(generate_uuid)" '._id = $id' /cats150.filter.json)
    # also edit the name field to be "cats150_$i"
    EDITED_FILTER_CONTENT=$(echo "$EDITED_FILTER_CONTENT" | jq --arg name "cats150_$i" '.name = $name')
    ADDED=$(mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" \
        --eval "db.filters.insertOne($EDITED_FILTER_CONTENT)")
    if [ $? -ne 0 ]; then
        echo "Failed to insert filter with filter_id $i: $ADDED"
        exit 1
    fi
done

# Now we load the ZTF_alerts_aux table with the history for all the objects detected on 2025-03-11,
echo "Loading ZTF_alerts_aux collection with data from boom_throughput.ZTF_alerts_aux.dump.gz"
mongorestore --uri="mongodb://mongoadmin:mongoadminsecret@mongo:27017/?authSource=admin" \
    --gzip \
    --archive=/boom_throughput.ZTF_alerts_aux.dump.gz \
    --nsInclude='boom_throughput.*' \
    --nsFrom='boom_throughput.*' \
    --nsTo="$DB_NAME.*"
mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" \
    --eval "db.ZTF_alerts_aux.createIndex({ 'coordinates.radec_geojson': '2dsphere' })"
    
# verify that we have the expected number of documents in the ZTF_alerts_aux collection
EXPECTED_AUX_ALERTS=27948
ACTUAL_AUX_ALERTS=$(mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" --quiet --eval "db.getSiblingDB('$DB_NAME').ZTF_alerts_aux.countDocuments()")
if [ "$ACTUAL_AUX_ALERTS" -ne "$EXPECTED_AUX_ALERTS" ]; then
    echo "Expected $EXPECTED_AUX_ALERTS documents in ZTF_alerts_aux collection, but found $ACTUAL_AUX_ALERTS"
    exit 1
else
    echo "Successfully loaded ZTF_alerts_aux collection with $ACTUAL_AUX_ALERTS documents"
fi

echo "MongoDB initialization script completed successfully"
exit 0
