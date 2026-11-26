#!/bin/bash
# Prerequisite: register the Hadoop table lab.silver.users in Polaris (dropped by the Polaris lab cleanup).
# Polaris REST API, principal root of the catalog; only a pointer to the metadata file is written, no data is copied.
set -e
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials \
  -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
V=$(aws s3 cp "s3://$LAB_BUCKET_NAME/warehouse/silver/users/metadata/version-hint.text" -)
echo "lab.silver.users current version: v$V"
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  "$POLARIS_URI/api/catalog/v1/$POLARIS_CATALOG/namespaces/silver/register" \
  -d "{\"name\": \"users\", \"metadata-location\": \"s3a://$LAB_BUCKET_NAME/warehouse/silver/users/metadata/v$V.metadata.json\"}" \
  | jq -c '{location: .metadata.location, columns: [.metadata.schemas[-1].fields[].name], error: .error}'
