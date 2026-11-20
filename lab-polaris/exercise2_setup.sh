#!/bin/bash
# Exercise 2: restricted catalog role silver_reader, plus a principal to test it
set -u
CRED_FILE=/home/onyxia/.polaris-credentials.env
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials \
  -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
RESP=$(mktemp)

call() {  # method, path, json body (sets $code and writes the response body in $RESP)
  code=$(curl -s -o "$RESP" -w "%{http_code}" -X "$1" "$API$2" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$3")
  echo "$1 $2 -> $code"
}

call POST "/catalogs/$POLARIS_CATALOG/catalog-roles" '{"catalogRole":{"name":"silver_reader","properties":{}}}'
for priv in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  call PUT "/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$priv\"}}"
done

call POST /principal-roles '{"principalRole":{"name":"silver_analyst","properties":{}}}'
call PUT "/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG" '{"catalogRole":{"name":"silver_reader"}}'

# The secret is only returned when the principal is created or reset: keep that response
call POST /principals '{"principal":{"name":"reader","properties":{}},"credentialRotationRequired":false}'
if [ "$code" != "201" ]; then call POST /principals/reader/reset '{}'; fi
SECRET=$(jq -r '"POLARIS_READER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' "$RESP")

call PUT /principals/reader/principal-roles '{"principalRole":{"name":"silver_analyst"}}'

# Store the credential next to the other ones, outside the repository
grep -v '^POLARIS_READER_CREDENTIAL=' "$CRED_FILE" > "$CRED_FILE.tmp" || true
echo "$SECRET" >> "$CRED_FILE.tmp"
mv "$CRED_FILE.tmp" "$CRED_FILE"; chmod 600 "$CRED_FILE"
rm -f "$RESP"
