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

# ingest N_FILTERS copies of the cats150 filter into filters collection
echo "Ingesting $N_FILTERS copies of the cats150 filter into filters collection"
for i in $(seq 1 $N_FILTERS); do
    echo "Inserting cats150 filter with filter_id $i into filters collection"
    # the file contains one document, so we read and edit the filter_id field
    EDITED_FILTER_CONTENT=$(jq --argjson id "$i" '.filter_id = $id' /cats150.json)
    ADDED=$(mongosh "mongodb://mongoadmin:mongoadminsecret@mongo:27017/$DB_NAME?authSource=admin" \
        --eval "db.filters.insertOne($EDITED_FILTER_CONTENT)")
    if [ $? -ne 0 ]; then
        echo "Failed to insert filter with filter_id $i: $ADDED"
        exit 1
    fi
done

echo "MongoDB initialization script completed successfully"
exit 0
