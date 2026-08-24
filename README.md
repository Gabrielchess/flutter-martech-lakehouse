# Case Técnico: Martech Specialist | Flutter Brazil

Pipeline que responde: **quais jogadores dormentes vale a pena reativar, com qual oferta, e quanto eles valem.**

## O problema

Duas coisas impediam a resposta: **Os valores estão em três moedas** e **A taxonomia de campanha está quebrada**.

## A resposta

Dos 250 jogadores, **123 são alvo acionável**: dormentes, com valor, liberados pelo compliance.

| | jogadores |
|---|---|
| base | 250 |
| − nunca ativaram | −7 |
| − ativos (≤30 dias) | −77 |
| − dormentes sem depósito confirmado | −28 |
| − bloqueados por compliance | −15 |
| **alvo acionável** | **123** |

> **[A PREENCHER]** — segmento recomendado, oferta, valor esperado.

## Como rodar

```bash
./build_lambdas.sh                               # empacota src/ em lambda/

cd infra/terraform
cp terraform.tfvars.example terraform.tfvars     # confira o pandas_layer_arn da região
terraform init && terraform apply

aws s3 cp ../../data/raw/ s3://flutter-martech-lakehouse/bronze/ --recursive --include "*.csv"
terraform output -raw comando_disparar_pipeline | bash

cd ../.. && python infra/sql/run_athena.py infra/sql/athena_gold.sql infra/sql/athena_silver.sql
```

Layout esperado em bronze: `bronze/{entidade}/ingest_date=YYYY-MM-DD/{entidade}.csv`.

## Arquitetura

![Arquitetura](docs/img/arquitetura-pipeline.png)

| Lambda | Lê → escreve | Faz |
|---|---|---|
| `flutter-fx` | Frankfurter → `reference/` | ingere cotações do BCE, materializa tabela diária densa |
| `flutter-silver` | `bronze/` → `silver/` | valida, deduplica, quarentena, tipa, converte para BRL, parseia taxonomia |
| `flutter-gold` | `silver/` → `gold/` | monta o star schema |

Step Functions em sequência, EventBridge Scheduler mensal, métricas via EMF no CloudWatch.

**O câmbio é uma Lambda separada** porque API é problema de ingestão, não de transformação. **A tabela de câmbio é densa**, contendo uma linha por dia corrido, com carry-forward explícito, porque o BCE não publica em fim de semana.

### Infraestrutura

Tudo provisionado por Terraform (`infra/terraform/`). Serverless por escolha: o volume não justifica cluster, e Lambda + Athena custam zero parado.

| Recurso | Configuração |
|---|---|
| **S3** | bucket único, versionado, SSE-S3, acesso público bloqueado. Lifecycle expira `athena-results/` em 7 dias e versões antigas em 30 |
| **Lambda** × 3 | Python 3.13, layer `AWSSDKPandas`, 512 MB (fx) / 1 GB (silver, gold), timeout 300 s |
| **IAM** | uma role por Lambda, cada uma com `Deny` explícito de `PutObject` em `bronze/*` |
| **Step Functions** | Standard, sequencial, `Retry` com backoff exponencial só em erro transitório |
| **EventBridge Scheduler** | `cron(0 3 1 * ? *)` — mensal, dia 1 às 03:00 UTC |
| **Glue Data Catalog** | database `flutter_martech`, alimentado por DDL explícito (sem crawler) |
| **Athena** | workgroup dedicado, output location fixo, teto de 1 GB de scan por query |
| **CloudWatch** | log groups com 30 dias de retenção, 3 alarmes, 1 dashboard |

```
bronze/{entidade}/ingest_date=YYYY-MM-DD/{entidade}.csv   imutável, particionado por carga
silver/{entidade}/{entidade}.parquet                       snappy, overwrite idempotente
gold/{tabela}/{tabela}.parquet
reference/fx_rates/current/rates.parquet                   ponteiro; snapshots datados ao lado
quarantine/{entidade}/ingest_date=…/reason=…/{run_id}.parquet
quality/dq_report/ingest_date=…/{run_id}.jsonl             JSONL, partition projection no Athena
```

Métricas saem por EMF — um `print` de JSON no stdout que o CloudWatch converte em métrica sozinho. Sem latência somada ao billing e sem throttle de `PutMetricData`. Alarmes: pipeline em `FAILED`, violação de DQ com severidade `ERROR`, câmbio em modo degradado.

## Modelo de dados

![Modelo gold](docs/img/modelo_logico_gold.png)

Star schema Kimball: `dim_date`, `dim_player`, `dim_campaign` + `fact_deposit`, `fact_bet`, `fact_touchpoint`. Três fatos porque são três processos com grãos diferentes, `dim_date` é gerada e não derivada dos fatos, senão faltariam justamente os dias sem movimento.

| Tabela | Tipo | Grão | Medidas |
|---|---|---|---|
| `dim_date` | dimensão | um dia corrido | — |
| `dim_player` | dimensão | um jogador | — |
| `dim_campaign` | dimensão | uma campanha | — |
| `fact_deposit` | transacional | uma tentativa de depósito | `amount`, `amount_brl` |
| `fact_bet` | transacional | uma aposta | `stake_brl`, `payout_brl`, `net_brl` |
| `fact_touchpoint` | sem medida (*factless*) | um evento de contato | contagem: `is_sent`, `is_open`, `is_click` |

**Fato nunca se junta a fato** — o encontro é nas dimensões conformadas (*drill-across*). Um `JOIN` direto entre `fact_deposit` e `fact_bet` multiplicaria linhas: 5 depósitos × 30 apostas = 150 linhas e receita inflada 30×.

**`date_key` é `int` no formato `yyyymmdd`**, não string nem timestamp: filtro de partição e `BETWEEN` ficam baratos e não existe ambiguidade de formato.

**Chaves naturais, sem SCD Tipo 2.** Nenhuma dimensão guarda histórico hoje; `deposit_id`, `bet_id` e `touchpoint_id` são dimensões degeneradas, carregadas no fato sem tabela própria.

**`currency`, `status`, `product` e `event_type` ficam inline no fato.** Cardinalidade 2–3 e nada pendurado neles — uma `dim_currency` de 3 linhas adicionaria um join e zero informação. Quando ganharem atributos próprios, viram uma *junk dimension*.

O agregado por jogador vive em `vw_player_360`. Com 250 jogadores, materializar seria uma tabela a manter em troca de milissegundos de scan, e duplicaria a régua de dormência em dois lugares. O custo assumido é não ter histórico de snapshot — quando a lista disparada precisar ser auditável meses depois, vira `fact_player_snapshot` com grão `(snapshot_date_key, player_id)`.

| View | Responde |
|---|---|
| `vw_player_360` | um jogador por linha: LTV, dormência, faixa de valor |
| `vw_segmentos_valor` | quantos jogadores e quanto valor em cada faixa |
| `vw_ltv_canal_oferta_produto` | LTV por canal × oferta × produto |
| `vw_conformidade_taxonomia` | aderência dos nomes ao padrão |
| `vw_dormencia_gaps` · `vw_dormencia_retorno` | evidência do limiar de 30 dias |

## Jogador dormente

```
atividade      = depósito confirmado OU aposta
dias_inativos  = date_diff('day', última atividade, 2024-04-01)

dormente       = dias_inativos > 30
```

**Atividade inclui aposta** porque apostar sem depositar é jogar com saldo: o jogador segue engajado, e ignorar isso o marcaria dormente sem motivo. Depósito `failed` ou `pending` não conta — o dinheiro não entrou —, mas a linha permanece na base: falha de pagamento é sinal de intenção.

**Por que 30.** É onde a taxa de retorno observada cruza os 50%: de 22 a 30 dias parados, 65,5% dos jogadores voltaram sozinhos; de 31 a 45, apenas 48,9%. Abaixo do limiar, gastar verba é pagar por quem ia voltar de graça. Reforço: 98,5% dos 2.715 intervalos entre atividades são ≤ 30 dias (p95 = 16, p99 = 38).

| Faixa | Critério | Leitura |
|---|---|---|
| `ativo` | ≤ 30 dias | volta sozinho |
| `dormente_30_60` | 31–60 | alvo primário |
| `dormente_60_84` | 61–84 | alvo, retorno já raro |
| `perdido_84_mais` | > 84 | das 82 pausas que passaram daqui, **nenhuma** terminou em retorno — é reaquisição |
| `nunca_ativou` | sem depósito e sem aposta | onboarding, não reativação: misturar inflaria o alvo |

**Limite reconhecido:** a régua é medida sobre uma janela de 8 meses. Um jogador de ciclo sazonal — que aposta só em Copa do Mundo — seria classificado como perdido.

## Definições adotadas

Vivem em um lugar só — a `vw_player_360` — para ninguém recalcular com limiar diferente.

| Conceito | Definição | Por quê |
|---|---|---|
| **Valor (LTV)** | soma dos depósitos **confirmados** em BRL, pela taxa da data da transação | GGR não serve: é negativo neste dataset, os payouts foram sorteados sem margem da casa |
| **Faixa de valor** | quartis de LTV entre os acionáveis | faixa fixa em reais quebra quando muda período ou câmbio; o corte relativo se ajusta |
| **Acionável** | dormente **e** LTV > 0 **e** não bloqueado | 26 jogadores estão fora por compliance: 14 autoexcluídos, 12 com KYC rejeitado |
| **Canal** | o do event stream, não o declarado no nome da campanha | nomes iguais para coisas diferentes é como se produz relatório errado |
| **Oferta** | a do último toque antes da data de referência | 1.258 toques não sustentam modelo multi-touch |

O detalhamento da régua de dormência (distribuição de intervalos, curva de retorno) está em `vw_dormencia_retorno`.

## Imperfeições tratadas

| Onde | O quê | Tratamento |
|---|---|---|
| `deposits` | 25 linhas byte-idênticas | quarentena, mantém a primeira |
| `players` | 22 sem `acquisition_channel` (8,8%) | vira `unknown` — descartar tiraria 1/10 do LTV por canal |
| `campaigns` | `C007` sem nome, `C008` fora do padrão | 0/7 segmentos, marcadas não-conformes |
| `campaigns` | `C002`, `C012` com separador ou ordem trocada | recuperados: o parser casa por vocabulário, não por posição |
| `campaigns` | `C005` com erro de grafia | recuperado por fuzzy match; empate vira `unknown`, nunca palpite |
| `campaigns` | `C004` sem `product` e `audience` | 5/7 segmentos |
| `touchpoints` | 2 eventos posteriores à data de referência | quarentena — usar toque futuro é vazamento temporal |
| `bets` | 2.080 linhas com `payout = 0` | mantidas: aposta perdida, não dado faltante |

Nada sai em silêncio: toda linha removida vai para `quarantine/` com o motivo, e um check garante `linhas_bronze == silver + quarentena`.

## Decisões técnicas

- **Data Quality é contrato, não script.** Cada entidade declara schema, chave, domínio, formato e integridade referencial. `ERROR` derruba o pipeline; `WARN` registra e segue. Onde a imperfeição é conhecida, o check é `WARN` com folga — é isso que faz o alarme significar "algo mudou" em vez de "esse dataset é sujo".
- **Chave duplicada com conteúdo diferente nunca é deduplicada.** Não dá para saber qual linha é a verdadeira: é `ERROR` e para.
- **Invariantes de travessia entre camadas.** A gold verifica que nenhum jogador some, que o fato preserva a contagem de linhas da silver, e que a **soma** em BRL atravessa intacta — contagem sozinha não pega merge que duplica valor mantendo o número de linhas.
- **A gold publica de forma atômica.** Ou o star inteiro vai, ou nada vai.
- **Parse de taxonomia não é posicional.** Os termos dos vocabulários são únicos entre si — se achou `casino`, só pode ser `product`. Leitura posicional quebraria em `BR_email_reactivation_2024Q1_bonus50`, onde a posição 4 viraria `product = '2024Q1'`.
- **`Retry` só em falha transitória de infra.** Repetir dado ruim gasta dinheiro para falhar igual.
- **Bronze é imutável para quem transforma.** As roles têm `Deny` explícito de escrita em `bronze/*`.

## Premissas

`preferred_currency` é atributo de perfil e não define a moeda da transação. Campanha arquivada continua explicando o passado — status não filtra a análise. Float64 com arredondamento a 2 casas basta para as magnitudes aqui (máx. ~2.000); com valores maiores, seria `decimal`.

## O que ficou de fora

**Ingestão de bronze** — os CSVs são carregados manualmente.
**Histórico de snapshot** — a view recalcula a cada consulta.
**Testes unitários** — o parser de taxonomia e o carry-forward de câmbio são onde pagariam primeiro.
**SCD Tipo 2** — `kyc_status` e `self_excluded` mudam com o tempo; o modelo só guarda o estado atual.
**Segmento de onboarding** — os 7 que nunca ativaram e os 28 sem depósito confirmado são público distinto, com oferta distinta.


## Estrutura

```
README.md                 entregável principal
build_lambdas.sh          empacota src/ nos zips de lambda/
src/
  shared/                 motor de DQ, câmbio, taxonomia, I/O e logging
  fx/ silver/ gold/       um handler por camada
lambda/                   os zips prontos para deploy
data/raw/                 os 5 CSVs de origem
infra/
  terraform/              infraestrutura como código
  sql/                    DDL, views e o runner do Athena
  aws/                    state machine e políticas IAM de referência
docs/                     apresentação, dicionário de dados, diagramas
```
