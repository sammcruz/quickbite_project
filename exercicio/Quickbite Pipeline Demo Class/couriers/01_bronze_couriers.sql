-- BRONZE: cópia fiel dos arquivos que chegam no landing.
-- Parâmetros do pipeline (Configuration): ${landing_path}, ${sla_minutes}, ${qb_catalog}, ${qb_schema}
-- Requisitos: veja a seção 3.1 do notebook.

CREATE OR REFRESH STREAMING TABLE bronze_couriers
select *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _ingest_ts
from STREAM read_files(
    concat(:landing_path, '/couriers/'),
    format => 'csv'
)
