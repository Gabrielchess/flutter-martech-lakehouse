-- =============================================================================
-- Athena | camada silver TIPADA e CONVERTIDA para BRL
--
-- ANTES DE RODAR: Athena -> Settings -> Manage -> Location of query result:
--   s3://flutter-martech-lakehouse/athena-results/
--
-- Silver agora sai da lambda com tipo de verdade e valor em BRL. Some o CAST
-- de toda consulta: `SUM(amount_brl)` funciona direto.
--
-- COLUNAS DE AUDITORIA DO CÂMBIO
--   fx_rate        taxa aplicada
--   fx_rate_date   dia em que o BCE publicou essa taxa (difere da data da
--                  transação em fim de semana e feriado)
--   *_brl          valor convertido pela taxa DA DATA DA TRANSAÇÃO
-- =============================================================================

CREATE DATABASE IF NOT EXISTS flutter_martech;

-- Rode os DROP sempre que o schema mudar. `CREATE ... IF NOT EXISTS` é NO-OP
-- em tabela existente: sem o DROP, a definição antiga permanece e você
-- consulta colunas que não existem mais no Parquet.
-- Tabela EXTERNA: o DROP remove só o metadado do catálogo. O dado no S3 fica.
DROP TABLE IF EXISTS flutter_martech.silver_players;
DROP TABLE IF EXISTS flutter_martech.silver_deposits;
DROP TABLE IF EXISTS flutter_martech.silver_bets;
DROP TABLE IF EXISTS flutter_martech.silver_campaigns;
DROP TABLE IF EXISTS flutter_martech.silver_campaign_touchpoints;
DROP TABLE IF EXISTS flutter_martech.fx_rates;
DROP TABLE IF EXISTS flutter_martech.quarantine_deposits;


CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.silver_players (
  player_id           string,
  signup_date         date,
  acquisition_channel string,   -- vazio virou 'unknown', nunca sumiu da base
  country             string,
  preferred_currency  string,   -- atributo de perfil; NÃO é a moeda da transação
  kyc_status          string,
  self_excluded       boolean,
  is_targetable       boolean   -- porta de compliance: exclui auto-excluído e KYC rejeitado
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/silver/players/';


CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.silver_deposits (
  deposit_id            string,
  player_id             string,
  deposit_ts            timestamp,
  amount                double,
  currency              string,
  status                string,
  fx_rate               double,
  fx_rate_date          string,
  amount_brl            double,
  is_confirmed          boolean   -- só 'confirmed' é dinheiro de verdade
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/silver/deposits/';


CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.silver_bets (
  bet_id                string,
  player_id             string,
  bet_ts                timestamp,
  stake                 double,
  currency              string,
  product               string,
  payout                double,
  fx_rate               double,
  fx_rate_date          string,
  stake_brl             double,
  payout_brl            double
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/silver/bets/';


CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.silver_campaigns (
  campaign_id       string,
  campaign_name     string,   -- original, intocado: é a evidência do relatório de conformidade
  created_date      date,
  status            string,
  geo               string,   -- BR | PT | AO | unknown
  channel           string,   -- declarado NO NOME; o do event stream é a fonte de verdade
  objective         string,
  product           string,
  audience          string,
  period            string,   -- YYYYQ#
  offer             string,   -- 'unknown' em C007 e C008, que carregam 170 touchpoints
  campaign_name_std string    -- reescrito no padrão; == campaign_name significa conforme
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/silver/campaigns/';


CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.silver_campaign_touchpoints (
  touchpoint_id string,
  player_id     string,
  campaign_id   string,
  channel       string,   -- fonte da verdade do canal, acima do nome da campanha
  event_ts      timestamp,
  event_type    string
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/silver/campaign_touchpoints/';


CREATE EXTERNAL TABLE IF NOT EXISTS flutter_martech.fx_rates (
  rate_date          string,
  from_currency      string,
  to_currency        string,
  rate               double,
  source_rate_date   string,
  is_carried_forward boolean,
  rate_source        string,
  fetched_at         string
)
STORED AS PARQUET
LOCATION 's3://flutter-martech-lakehouse/reference/fx_rates/current/';


-- =============================================================================
-- 1. Sanidade de volume
-- =============================================================================
SELECT 'players' AS entidade, count(*) AS linhas FROM flutter_martech.silver_players
UNION ALL SELECT 'deposits',    count(*) FROM flutter_martech.silver_deposits
UNION ALL SELECT 'bets',        count(*) FROM flutter_martech.silver_bets
UNION ALL SELECT 'campaigns',   count(*) FROM flutter_martech.silver_campaigns
UNION ALL SELECT 'touchpoints', count(*) FROM flutter_martech.silver_campaign_touchpoints
ORDER BY 1;
-- esperado: players 250 | deposits 1151 | bets 3702 | campaigns 12 | touchpoints 1258


-- =============================================================================
-- 2. A CONVERSÃO FUNCIONOU? Nenhuma linha pode ter ficado sem taxa.
--    Se vier diferente de zero, tem receita sumindo da soma sem erro nenhum.
-- =============================================================================
SELECT sum(CASE WHEN fx_rate IS NULL THEN 1 ELSE 0 END) AS depositos_sem_taxa,
       (SELECT sum(CASE WHEN fx_rate IS NULL THEN 1 ELSE 0 END)
          FROM flutter_martech.silver_bets)             AS apostas_sem_taxa
FROM flutter_martech.silver_deposits;
-- esperado: 0 e 0


-- =============================================================================
-- 3. Depósito confirmado em BRL, por moeda de origem
--    Sem CAST: a coluna já é double e já está convertida.
-- =============================================================================
SELECT currency,
       count(*)                     AS depositos,
       round(sum(amount), 2)        AS total_moeda_original,
       round(sum(amount_brl), 2)    AS total_brl
FROM flutter_martech.silver_deposits
WHERE is_confirmed
GROUP BY currency
ORDER BY total_brl DESC;


-- =============================================================================
-- 4. Prévia do LTV por canal de aquisição — a pergunta do case
--    'unknown' aparece porque os 22 sem canal continuam na base.
-- =============================================================================
SELECT p.acquisition_channel,
       count(DISTINCT p.player_id)          AS jogadores,
       round(sum(d.amount_brl), 2)          AS depositado_brl,
       round(sum(d.amount_brl)
             / count(DISTINCT p.player_id), 2) AS por_jogador
FROM flutter_martech.silver_players p
JOIN flutter_martech.silver_deposits d ON d.player_id = p.player_id
WHERE d.is_confirmed
GROUP BY p.acquisition_channel
ORDER BY depositado_brl DESC;


-- =============================================================================
-- 5. Sanidade do resultado por produto
--    Um operador real retém +5% a +10% do apostado. Aqui o resultado é
--    negativo: os payouts foram sorteados sem margem da casa. Por isso o
--    depósito confirmado, e não o resultado de jogo, é a medida de valor.
-- =============================================================================
SELECT product,
       count(*)                                            AS apostas,
       round(sum(stake_brl), 2)                            AS apostado_brl,
       round(sum(payout_brl), 2)                           AS pago_brl,
       round(sum(stake_brl) - sum(payout_brl), 2)          AS resultado_brl,
       round(100.0 * (sum(stake_brl) - sum(payout_brl))
                   / sum(stake_brl), 2)                    AS retencao_pct
FROM flutter_martech.silver_bets
GROUP BY product
ORDER BY resultado_brl;


-- =============================================================================
-- 6. Compliance: quem NÃO pode entrar em lista de campanha
-- =============================================================================
SELECT is_targetable, self_excluded, kyc_status, count(*) AS jogadores
FROM flutter_martech.silver_players
GROUP BY is_targetable, self_excluded, kyc_status
ORDER BY is_targetable, jogadores DESC;


-- =============================================================================
-- 7. Relatório de conformidade da taxonomia
--    Sem colunas de metadado: conformidade é a comparação com o nome
--    padronizado, e a contagem de segmentos sai de contar os 'unknown'.
-- =============================================================================
SELECT campaign_id,
       campaign_name,
       campaign_name_std,
       (campaign_name = campaign_name_std)                    AS conforme,
       7 - cardinality(filter(ARRAY[geo, channel, objective, product,
                                    audience, period, offer],
                              x -> x = 'unknown'))            AS segmentos_resolvidos
FROM flutter_martech.silver_campaigns
ORDER BY segmentos_resolvidos, campaign_id;
-- esperado: 4 conformes | C004 com 5/7 | C007 e C008 com 0/7


-- =============================================================================
-- 8. LTV por canal de aquisição x oferta x produto — a pergunta da seção 3b.3
--    Só possível porque a taxonomia virou coluna.
-- =============================================================================
SELECT p.acquisition_channel,
       c.offer,
       c.product,
       count(DISTINCT p.player_id)   AS jogadores,
       round(sum(d.amount_brl), 2)   AS depositado_brl
FROM flutter_martech.silver_players p
JOIN flutter_martech.silver_campaign_touchpoints t ON t.player_id = p.player_id
JOIN flutter_martech.silver_campaigns c            ON c.campaign_id = t.campaign_id
JOIN flutter_martech.silver_deposits d             ON d.player_id = p.player_id
WHERE d.is_confirmed AND t.event_type = 'click'
GROUP BY p.acquisition_channel, c.offer, c.product
ORDER BY depositado_brl DESC;
