#!/usr/bin/env bash
set -euo pipefail

# Helper: call Trino CLI inside the container
trinoi() {
  docker exec -i trino-fg trino \
    --server http://localhost:8080 \
    --catalog iceberg \
    --schema datasets \
    --user web \
    "$@"
}

# Example: GitHub Releases, with sleeps between steps
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.github_releases_1 (
  id BIGINT,
  repo VARCHAR,
  tag_name VARCHAR,
  name VARCHAR,
  draft BOOLEAN,
  prerelease BOOLEAN,
  published_at TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id, name, source, created_at, row_count, size_bytes, last_profiled_at)
VALUES ('github_releases_1','GitHub Releases','GitHub', current_timestamp, NULL, NULL, current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id, column_name, data_type, pii_flag, null_ratio, distinct_ratio, indexed)
SELECT
  'github_releases_1',
  c.column_name,
  c.data_type,
  FALSE,
  NULL,
  NULL,
  CASE
    WHEN c.column_name IN ('id') THEN TRUE
    WHEN c.column_name IN ('published_at') THEN TRUE
    ELSE FALSE
  END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg'
  AND c.table_schema='datasets'
  AND c.table_name='github_releases_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id, dst_dataset_id, transform_type, updated_at)
VALUES ('github_releases_raw','github_releases_1','Replication', current_timestamp)"
