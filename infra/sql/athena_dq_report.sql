-- =============================================================================
-- Tabela Athena sobre o relatorio de Data Quality
-- Rode uma vez no console do Athena (Query editor).
--
-- Usa PARTITION PROJECTION: o Athena calcula as particoes pelo padrao do
-- caminho, entao nao precisa de Glue Crawler nem de MSCK REPAIR TABLE a cada
-- carga. Uma peca a menos para manter e para quebrar.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS dq_report (
  entity          string,
  `check`         string,   -- palavra reservada: sempre entre crases
  `column`        string,   -- idem
  severity        string,   -- ERROR | WARN
  passed          boolean,  -- false = estourou a tolerancia do contrato
  failed_records  int,      -- quantas LINHAS o check tocou
  total_records   int,
  failed_ratio    double,
  details         string,
  run_id          string,
  checked_at      string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://flutter-martech-lakehouse/quality/dq_report/'
TBLPROPERTIES (
  'projection.enabled'                = 'true',
  'projection.ingest_date.type'       = 'date',
  'projection.ingest_date.format'     = 'yyyy-MM-dd',
  'projection.ingest_date.range'      = '2024-01-01,NOW',
  'projection.ingest_date.interval'   = '1',
  'projection.ingest_date.interval.unit' = 'DAYS',
  'storage.location.template'         = 's3://flutter-martech-lakehouse/quality/dq_report/ingest_date=${ingest_date}/'
);


-- =============================================================================
-- 1. Veredito de cada execucao
-- =============================================================================
SELECT ingest_date,
       run_id,
       count(*)                                          AS checks_rodados,
       count_if(NOT passed AND severity = 'ERROR')        AS reprovou_error,
       count_if(NOT passed AND severity = 'WARN')         AS reprovou_warn
FROM dq_report
GROUP BY ingest_date, run_id
ORDER BY ingest_date DESC;


-- =============================================================================
-- 2. Tendencia por coluna — a pergunta que o CloudWatch nao responde
--    "a imperfeicao de acquisition_channel esta piorando mes a mes?"
-- =============================================================================
SELECT ingest_date,
       entity,
       `column`,
       `check`,
       failed_records,
       round(failed_ratio * 100, 2) AS pct
FROM dq_report
WHERE failed_records > 0
ORDER BY entity, `column`, ingest_date;


-- =============================================================================
-- 3. Checks que MUDARAM de veredito entre execucoes
--    O alarme avisa que quebrou hoje; isto mostra QUANDO comecou a quebrar.
-- =============================================================================
WITH hist AS (
  SELECT entity, `column`, `check`, ingest_date, passed, failed_records,
         lag(passed)         OVER (PARTITION BY entity, `column`, `check` ORDER BY ingest_date) AS passed_anterior,
         lag(failed_records) OVER (PARTITION BY entity, `column`, `check` ORDER BY ingest_date) AS registros_anterior
  FROM dq_report
)
SELECT *
FROM hist
WHERE passed_anterior IS NOT NULL
  AND passed <> passed_anterior
ORDER BY ingest_date DESC;
