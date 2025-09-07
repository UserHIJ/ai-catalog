#!/usr/bin/env bash
# reset_nessie_minio.sh — clean rebuild of Nessie (Iceberg REST) + MinIO warehouse
# WARNING: By default this nukes your warehouse bucket contents.

set -euo pipefail

### --- config you can tweak ---
NESSIE_CONTAINER="ops-nessie-1"
NESSIE_IMAGE="ghcr.io/projectnessie/nessie:0.99.0"
NESSIE_PORT=19120

MINIO_ALIAS="local"
MINIO_ENDPOINT="http://host.docker.internal:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_REGION="us-west-1"

WAREHOUSE_BUCKET="iceberg-warehouse"          # s3://<bucket>/
WAREHOUSE_NAME="warehouse"                    # Nessie “warehouse” logical name
NUKE_WAREHOUSE="true"                         # set to "false" to keep existing files
### -----------------------------------------

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing $1"; exit 1; }; }
need docker
need curl
need mc

echo "→ ensuring MinIO alias '${MINIO_ALIAS}'"
mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" >/dev/null

if [[ "${NUKE_WAREHOUSE}" == "true" ]]; then
  echo "⚠️  nuking s3://${WAREHOUSE_BUCKET}/ (all Iceberg tables & metadata)"
  mc rm -r --force "${MINIO_ALIAS}/${WAREHOUSE_BUCKET}" >/dev/null || true
fi

# ensure bucket exists
if ! mc ls "${MINIO_ALIAS}/${WAREHOUSE_BUCKET}" >/dev/null 2>&1; then
  echo "→ creating bucket s3://${WAREHOUSE_BUCKET}/"
  mc mb "${MINIO_ALIAS}/${WAREHOUSE_BUCKET}" >/dev/null
fi

echo "→ restarting Nessie (${NESSIE_CONTAINER}) with Iceberg REST config"
docker rm -f "${NESSIE_CONTAINER}" >/dev/null 2>&1 || true

docker run -d --name "${NESSIE_CONTAINER}" -p ${NESSIE_PORT}:${NESSIE_PORT} \
  -e QUARKUS_HTTP_PORT=${NESSIE_PORT} \
  -e nessie.version.store.type=IN_MEMORY \
  -e nessie.catalog.default-warehouse="${WAREHOUSE_NAME}" \
  -e nessie.catalog.warehouses.${WAREHOUSE_NAME}.location="s3://${WAREHOUSE_BUCKET}/" \
  -e nessie.catalog.service.s3.default-options.endpoint="${MINIO_ENDPOINT}" \
  -e nessie.catalog.service.s3.default-options.path-style-access=true \
  -e nessie.catalog.service.s3.default-options.region="${MINIO_REGION}" \
  -e nessie.catalog.service.s3.default-options.auth-type=STATIC \
  -e nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:minio-creds \
  -e minio-creds.name="${MINIO_ACCESS_KEY}" \
  -e minio-creds.secret="${MINIO_SECRET_KEY}" \
  "${NESSIE_IMAGE}" >/dev/null

echo "→ waiting for Nessie REST to serve catalog config…"
for i in {1..30}; do
  if curl -sf "http://host.docker.internal:${NESSIE_PORT}/iceberg/v1/config?warehouse=${WAREHOUSE_NAME}" >/dev/null; then
    echo "✅ Nessie Iceberg REST is up"
    break
  fi
  sleep 1
  [[ $i -eq 30 ]] && { echo "❌ Nessie REST not responding"; exit 1; }
done

echo
echo "Next (DuckDB):"
cat <<'SQL'
-- in duckdb
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

-- MinIO creds (match your env)
SET s3_endpoint='http://host.docker.internal:9000';
SET s3_region='us-west-1';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key="minioadmin";
SET s3_url_style='path';

-- cheap bearer to satisfy client (Nessie ignores if no auth configured)
CREATE SECRET nessie_noauth (TYPE ICEBERG, TOKEN 'dev');

-- attach the Nessie Iceberg REST catalog (no more version-hint hacks)
ATTACH 'warehouse' AS nessie_iceberg (
  TYPE iceberg,
  ENDPOINT 'http://host.docker.internal:19120/iceberg',
  SECRET nessie_noauth
);

SHOW ALL TABLES;
SQL
echo
echo "Done."

