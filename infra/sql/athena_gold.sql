CREATE DATABASE IF NOT EXISTS flutter_martech;

DROP TABLE IF EXISTS flutter_martech.dim_date;

DROP TABLE IF EXISTS flutter_martech.dim_player;

DROP TABLE IF EXISTS flutter_martech.dim_campaign;

DROP TABLE IF EXISTS flutter_martech.fact_deposit;

DROP TABLE IF EXISTS flutter_martech.fact_bet;

DROP TABLE IF EXISTS flutter_martech.fact_touchpoint;

CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.dim_date (
  date_key        int,
  full_date       string,
  `year`          int,
  quarter         string,
  `month`         int,
  day_of_week     int,
  day_name        string,
  is_weekend      boolean,
  is_business_day boolean
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/gold/dim_date/';

CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.dim_player (
  player_id           string,
  signup_date_key     int,
  acquisition_channel string,
  country             string,
  preferred_currency  string,
  kyc_status          string,
  self_excluded       boolean,
  is_targetable       boolean
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/gold/dim_player/';

CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.dim_campaign (
  campaign_id           string,
  campaign_name         string,
  campaign_name_std     string,
  created_date_key      int,
  status                string,
  geo                   string,
  channel_declarado     string,
  objective             string,
  product               string,
  audience              string,
  period                string,
  offer                 string,
  is_taxonomy_compliant boolean
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/gold/dim_campaign/';

CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.fact_deposit (
  deposit_id   string,
  player_id    string,
  date_key     int,
  deposit_ts   timestamp,
  currency     string,
  status       string,
  is_confirmed boolean,
  amount       double,
  fx_rate      double,
  amount_brl   double
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/gold/fact_deposit/';

CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.fact_bet (
  bet_id      string,
  player_id   string,
  date_key    int,
  bet_ts      timestamp,
  product     string,
  currency    string,
  stake       double,
  payout      double,
  fx_rate     double,
  stake_brl   double,
  payout_brl  double,
  net_brl     double
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/gold/fact_bet/';

CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.fact_touchpoint (
  touchpoint_id    string,
  player_id        string,
  campaign_id      string,
  date_key         int,
  event_ts         timestamp,
  delivery_channel string,
  event_type       string,
  is_sent          int,
  is_open          int,
  is_click         int
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/gold/fact_touchpoint/';

CREATE OR REPLACE VIEW flutter_martech.vw_player_360 AS
WITH params AS (
  SELECT TIMESTAMP '2024-04-01 00:00:00' AS ref_ts,
         30 AS dormancy_days
),
dep AS (
  SELECT player_id,
         sum(amount_brl) AS ltv_brl,
         count(*)        AS deposit_count,
         max(deposit_ts) AS last_deposit
  FROM flutter_martech.fact_deposit
  WHERE is_confirmed
  GROUP BY player_id
),
bet AS (
  SELECT player_id,
         sum(stake_brl) AS total_stake_brl,
         sum(net_brl)   AS net_brl,
         count(*)       AS bet_count,
         max(bet_ts)    AS last_bet
  FROM flutter_martech.fact_bet
  GROUP BY player_id
),
prod AS (
  SELECT player_id, product AS primary_product
  FROM (
    SELECT player_id, product,
           row_number() OVER (PARTITION BY player_id ORDER BY sum(stake_brl) DESC) AS rn
    FROM flutter_martech.fact_bet
    GROUP BY player_id, product
  )
  WHERE rn = 1
),
base AS (
  SELECT
    pl.player_id,
    pl.country,
    pl.acquisition_channel,
    pl.kyc_status,
    pl.self_excluded,
    pl.is_targetable,
    coalesce(pr.primary_product, 'nenhum')   AS primary_product,
    coalesce(d.deposit_count, 0)             AS deposit_count,
    coalesce(b.bet_count, 0)                 AS bet_count,
    round(coalesce(d.ltv_brl, 0), 2)         AS ltv_brl,
    round(coalesce(b.total_stake_brl, 0), 2) AS total_stake_brl,
    round(coalesce(b.net_brl, 0), 2)         AS net_brl,
    coalesce(greatest(d.last_deposit, b.last_bet), d.last_deposit, b.last_bet) AS last_activity_ts
  FROM flutter_martech.dim_player pl
  LEFT JOIN dep  d  ON d.player_id  = pl.player_id
  LEFT JOIN bet  b  ON b.player_id  = pl.player_id
  LEFT JOIN prod pr ON pr.player_id = pl.player_id
),
dorm AS (
  SELECT b.*,
         date_diff('day', b.last_activity_ts, p.ref_ts) AS days_inactive,
         date_diff('day', b.last_activity_ts, p.ref_ts) > p.dormancy_days AS is_dormant
  FROM base b
  CROSS JOIN params p
),
tiers AS (
  SELECT player_id, ntile(4) OVER (ORDER BY ltv_brl) AS t
  FROM dorm
  WHERE is_dormant AND ltv_brl > 0 AND is_targetable
)
SELECT d.player_id,
       d.country,
       d.acquisition_channel,
       d.primary_product,
       d.kyc_status,
       d.self_excluded,
       d.is_dormant,
       d.days_inactive,
       CASE t.t WHEN 1 THEN 'Q1_baixo' WHEN 2 THEN 'Q2_medio'
                WHEN 3 THEN 'Q3_alto'  WHEN 4 THEN 'Q4_muito_alto'
                ELSE 'sem_faixa' END AS value_tier,
       d.deposit_count,
       d.bet_count,
       d.ltv_brl,
       d.total_stake_brl,
       d.net_brl
FROM dorm d
LEFT JOIN tiers t ON t.player_id = d.player_id;

CREATE OR REPLACE VIEW flutter_martech.vw_segmentos_valor AS
SELECT p.value_tier,
       count(*)                            AS jogadores,
       round(min(p.ltv_brl), 2)            AS ltv_min_brl,
       round(avg(p.ltv_brl), 2)            AS ltv_medio_brl,
       round(sum(p.ltv_brl), 2)            AS ltv_total_brl,
       round(100.0 * sum(p.ltv_brl) / sum(sum(p.ltv_brl)) OVER (), 1) AS pct_do_ltv_alvo,
       round(avg(p.deposit_count), 1)      AS depositos_medio
FROM flutter_martech.vw_player_360 p
WHERE p.is_dormant
  AND p.ltv_brl > 0
  AND NOT p.self_excluded
  AND p.kyc_status <> 'rejected'
GROUP BY p.value_tier;

CREATE OR REPLACE VIEW flutter_martech.vw_ltv_canal_oferta_produto AS
WITH last_offer AS (
  SELECT player_id, offer
  FROM (
    SELECT t.player_id, c.offer,
           row_number() OVER (PARTITION BY t.player_id ORDER BY t.event_ts DESC) AS rn
    FROM flutter_martech.fact_touchpoint t
    JOIN flutter_martech.dim_campaign c ON c.campaign_id = t.campaign_id
    WHERE t.event_ts < TIMESTAMP '2024-04-01 00:00:00'
  )
  WHERE rn = 1
)
SELECT p.acquisition_channel,
       coalesce(o.offer, 'sem_contato')              AS offer,
       p.primary_product,
       count(*)                                      AS jogadores,
       sum(CASE WHEN p.is_dormant THEN 1 ELSE 0 END) AS dormentes,
       sum(CASE WHEN p.is_dormant AND p.ltv_brl > 0
                 AND NOT p.self_excluded AND p.kyc_status <> 'rejected'
                THEN 1 ELSE 0 END)                   AS acionaveis,
       round(sum(p.ltv_brl), 2)                      AS ltv_total_brl,
       round(avg(p.ltv_brl), 2)                      AS ltv_medio_brl
FROM flutter_martech.vw_player_360 p
LEFT JOIN last_offer o ON o.player_id = p.player_id
GROUP BY p.acquisition_channel, coalesce(o.offer, 'sem_contato'), p.primary_product;

CREATE OR REPLACE VIEW flutter_martech.vw_conformidade_taxonomia AS
SELECT c.campaign_id,
       c.campaign_name,
       c.campaign_name_std,
       c.is_taxonomy_compliant,
       7 - cardinality(filter(ARRAY[c.geo, c.channel_declarado, c.objective,
                                    c.product, c.audience, c.period, c.offer],
                              x -> x = 'unknown')) AS segmentos_resolvidos,
       count(t.touchpoint_id)                      AS touchpoints_afetados
FROM flutter_martech.dim_campaign c
LEFT JOIN flutter_martech.fact_touchpoint t ON t.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.campaign_name_std,
         c.is_taxonomy_compliant, c.geo, c.channel_declarado, c.objective,
         c.product, c.audience, c.period, c.offer;

CREATE OR REPLACE VIEW flutter_martech.vw_dormencia_gaps AS
WITH atividade AS (
  SELECT player_id, CAST(deposit_ts AS date) AS d
  FROM flutter_martech.fact_deposit
  WHERE is_confirmed
  UNION
  SELECT player_id, CAST(bet_ts AS date)
  FROM flutter_martech.fact_bet
),
gaps AS (
  SELECT date_diff('day', lag(d) OVER (PARTITION BY player_id ORDER BY d), d) AS gap_dias
  FROM atividade
),
faixas AS (
  SELECT CASE
           WHEN gap_dias <=  7 THEN '01_ate_7'
           WHEN gap_dias <= 14 THEN '02_8_14'
           WHEN gap_dias <= 21 THEN '03_15_21'
           WHEN gap_dias <= 30 THEN '04_22_30'
           WHEN gap_dias <= 45 THEN '05_31_45'
           WHEN gap_dias <= 60 THEN '06_46_60'
           WHEN gap_dias <= 84 THEN '07_61_84'
           ELSE '08_85_mais'
         END AS faixa_dias,
         gap_dias
  FROM gaps
  WHERE gap_dias IS NOT NULL
)
SELECT faixa_dias,
       count(*)                                                     AS intervalos,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)           AS pct,
       round(100.0 * sum(count(*)) OVER (ORDER BY faixa_dias)
                   / sum(count(*)) OVER (), 2)                      AS pct_acumulado,
       max(gap_dias)                                                AS maior_gap_da_faixa
FROM faixas
GROUP BY faixa_dias;

CREATE OR REPLACE VIEW flutter_martech.vw_dormencia_retorno AS
WITH atividade AS (
  SELECT player_id, CAST(deposit_ts AS date) AS d
  FROM flutter_martech.fact_deposit
  WHERE is_confirmed
  UNION
  SELECT player_id, CAST(bet_ts AS date)
  FROM flutter_martech.fact_bet
),
pausas AS (
  SELECT date_diff('day', lag(d) OVER (PARTITION BY player_id ORDER BY d), d) AS dias_parado,
         1 AS voltou
  FROM atividade
  UNION ALL
  SELECT date_diff('day', max(d), DATE '2024-04-01'), 0
  FROM atividade
  GROUP BY player_id
)
SELECT CASE
         WHEN dias_parado <=  7 THEN '01_ate_7'
         WHEN dias_parado <= 14 THEN '02_8_14'
         WHEN dias_parado <= 21 THEN '03_15_21'
         WHEN dias_parado <= 30 THEN '04_22_30'
         WHEN dias_parado <= 45 THEN '05_31_45'
         WHEN dias_parado <= 60 THEN '06_46_60'
         WHEN dias_parado <= 84 THEN '07_61_84'
         ELSE '08_85_mais'
       END                                                  AS faixa_dias,
       count(*)                                             AS pausas,
       sum(voltou)                                          AS voltaram,
       round(100.0 * sum(voltou) / count(*), 1)             AS taxa_retorno_pct
FROM pausas
WHERE dias_parado IS NOT NULL
GROUP BY 1;
