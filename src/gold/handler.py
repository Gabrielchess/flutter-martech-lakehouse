"""
silver -> gold | Lambda 3 do pipeline Martech (Flutter Brazil)

MODELO DIMENSIONAL (star schema)
--------------------------------
A silver é o espelho limpo da origem: uma tabela por arquivo, mesma granularidade.
A gold reorganiza esse dado por PROCESSO DE NEGÓCIO, no padrão Kimball.

  DIMENSÕES (conformadas — compartilhadas por todos os fatos)
    dim_date       um dia
    dim_player     um jogador
    dim_campaign   uma campanha, com a taxonomia já parseada

  FATOS
    fact_deposit      transacional | uma tentativa de depósito
    fact_bet          transacional | uma aposta
    fact_touchpoint   de evento    | um envio/abertura/clique (sem medida
                                     aditiva — a métrica é a CONTAGEM)

Três fatos porque são três processos distintos, cada um com sua granularidade.
Fato não se junta a fato: eles se encontram nas dimensões conformadas (padrão
drill-across, exemplo no rodapé de athena_gold.sql).

O QUE ESTA CAMADA NÃO FAZ
-------------------------
Não materializa agregado por jogador. LTV, dias inativos, dormência e faixa de
valor são SEMÂNTICA DE CONSUMO, não modelagem: vivem em vw_player_360
(athena_gold.sql). Com 250 jogadores, materializar isso seria uma tabela a mais
para manter em troca de milissegundos de scan. E a régua de dormência fica
declarada em UM lugar — a view — em vez de em dois.

O trade-off assumido: sem tabela materializada não há histórico de snapshot.
Recalcular a view em junho dá resultado diferente do de abril, porque
days_inactive cresce e os tercis se deslocam. No dia em que a campanha virar
mensal e a lista disparada precisar ser auditável, isso vira
fact_player_snapshot com grão (snapshot_date_key, player_id) — e aí a
materialização se justifica.

ATRIBUTOS DESCRITIVOS NOS FATOS
-------------------------------
currency, status, product, event_type e delivery_channel ficam INLINE no fato,
sem dimensão própria. É escolha, não descuido: cardinalidade 2-3 e nada
pendurado neles. Uma dim_currency de 3 linhas adicionaria um join e zero
informação. Se um dia ganharem atributos, viram uma junk dimension.

CHAVES: naturais (player_id, campaign_id), não substitutas. Não há SCD Tipo 2
aqui — nenhuma dimensão guarda histórico. No dia em que kyc_status precisar de
versionamento (e vai precisar), entram surrogate keys e uma faixa de validade;
antes disso, a chave sintética só adiciona um passo de geração que pode falhar.

Runtime: Python 3.14
Layers:  AWSSDKPandas-Python314 (v11)
Env:     LAKEHOUSE_BUCKET=flutter-martech-lakehouse   (obrigatória)
         REFERENCE_DATE=2024-04-01     "hoje" do case
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd

from shared import data_quality as dq
from shared import logger, s3_io
from shared.data_quality import ERROR, WARN

BUCKET = os.environ["LAKEHOUSE_BUCKET"]
REFERENCE_DATE = os.environ.get("REFERENCE_DATE", "2024-04-01")
FAIL_ON_ERROR = os.environ.get("DQ_FAIL_ON_ERROR", "true").lower() == "true"

CHANNELS = {"organic", "influencer", "seo", "paid_social", "affiliate", "unknown"}
DELIVERY = {"email", "push", "sms"}


# =============================================================================
# CONTRATOS DE QUALIDADE — um por tabela do star
# =============================================================================
# Nas dimensões, `pk` é literal: a chave é única por definição do grão.
# Nos fatos, `pk` é a chave degenerada (id da transação de origem) e as `fks`
# apontam para as dimensões — órfão aqui significa fato que não vai aparecer
# em nenhum relatório depois do join, o defeito mais silencioso de um star.
CONTRACTS = {
    "dim_date": {
        "columns": ["date_key", "full_date", "year", "quarter", "month",
                    "day_of_week", "day_name", "is_weekend", "is_business_day"],
        "pk": "date_key",
        "not_blank": {"date_key": (ERROR, 0.0), "full_date": (ERROR, 0.0)},
        "positive": ["date_key", "year"],
    },
    "dim_player": {
        "columns": ["player_id", "signup_date_key", "acquisition_channel", "country",
                    "preferred_currency", "kyc_status", "self_excluded", "is_targetable"],
        "pk": "player_id",
        "expected_rows": 250,
        "not_blank": {"player_id": (ERROR, 0.0), "acquisition_channel": (ERROR, 0.0)},
        "domains": {"acquisition_channel": (WARN, CHANNELS)},
        "pattern": {"player_id": r"P\d{5}"},
        "fks": {"signup_date_key": "dim_date.date_key"},
    },
    "dim_campaign": {
        "columns": ["campaign_id", "campaign_name", "campaign_name_std",
                    "created_date_key", "status", "geo", "channel_declarado",
                    "objective", "product", "audience", "period", "offer",
                    "is_taxonomy_compliant"],
        "pk": "campaign_id",
        "not_blank": {"campaign_id": (ERROR, 0.0)},
        # campaign_name PODE ser vazio (C007) — é o defeito que o case quer ver
        # tratado, não um motivo para derrubar a carga. Vira 'unknown' nos
        # segmentos e fica registrado em is_taxonomy_compliant.
        "domains": {"offer": (WARN, {"bonus50", "bonus100", "freebet",
                                     "freespins", "cashback", "none", "unknown"}),
                    "product": (WARN, {"sports", "casino", "both", "unknown"})},
        "fks": {"created_date_key": "dim_date.date_key"},
    },
    "fact_deposit": {
        "columns": ["deposit_id", "player_id", "date_key", "deposit_ts", "currency",
                    "status", "is_confirmed", "amount", "fx_rate", "amount_brl"],
        "pk": "deposit_id",
        "not_blank": {"deposit_id": (ERROR, 0.0), "player_id": (ERROR, 0.0)},
        "domains": {"currency": (ERROR, {"BRL", "EUR", "USD"}),
                    "status": (ERROR, {"confirmed", "pending", "failed"})},
        "positive": ["amount", "amount_brl", "fx_rate"],
        "fks": {"player_id": "dim_player.player_id", "date_key": "dim_date.date_key"},
    },
    "fact_bet": {
        "columns": ["bet_id", "player_id", "date_key", "bet_ts", "product", "currency",
                    "stake", "payout", "fx_rate", "stake_brl", "payout_brl", "net_brl"],
        "pk": "bet_id",
        "not_blank": {"bet_id": (ERROR, 0.0), "player_id": (ERROR, 0.0)},
        "domains": {"currency": (ERROR, {"BRL", "EUR", "USD"}),
                    "product": (ERROR, {"sports", "casino"})},
        "positive": ["stake", "stake_brl", "fx_rate"],
        "non_negative": ["payout", "payout_brl"],   # net_brl fica de fora: negativo é legítimo
        "fks": {"player_id": "dim_player.player_id", "date_key": "dim_date.date_key"},
    },
    "fact_touchpoint": {
        "columns": ["touchpoint_id", "player_id", "campaign_id", "date_key", "event_ts",
                    "delivery_channel", "event_type", "is_sent", "is_open", "is_click"],
        "pk": "touchpoint_id",
        "not_blank": {"touchpoint_id": (ERROR, 0.0), "player_id": (ERROR, 0.0),
                      "campaign_id": (ERROR, 0.0)},
        "domains": {"delivery_channel": (ERROR, DELIVERY),
                    "event_type": (ERROR, {"sent", "open", "click"})},
        "fks": {"player_id": "dim_player.player_id",
                "campaign_id": "dim_campaign.campaign_id",
                "date_key": "dim_date.date_key"},
    },
}

# Ordem de gravação: dimensões antes dos fatos. Não é estética — é o que garante
# que um leitor que pegue o bucket no meio da execução nunca ache fato órfão.
WRITE_ORDER = ["dim_date", "dim_player", "dim_campaign",
               "fact_deposit", "fact_bet", "fact_touchpoint"]


def _date_key(s):
    """timestamp/date -> int yyyymmdd. Inteiro, não string: o filtro de
    partição e o BETWEEN ficam baratos, e não existe ambiguidade de formato."""
    return pd.to_datetime(s, utc=True, errors="coerce").dt.strftime("%Y%m%d").astype("Int64")


# =============================================================================
# DIMENSÕES
# =============================================================================
def build_dim_date(frames, ref):
    """
    Gerada, não extraída. Uma dimensão de data derivada dos fatos teria buracos
    exatamente nos dias sem movimento — e aí "quantos dias sem depósito?" viraria
    uma pergunta impossível, porque o dia ausente não existe para contar.
    Cobre do primeiro evento ao último, esticada até a data de referência.
    """
    stamps = [
        frames["deposits"].deposit_ts, frames["bets"].bet_ts,
        frames["campaign_touchpoints"].event_ts,
        pd.to_datetime(frames["players"].signup_date, utc=True, errors="coerce"),
        pd.to_datetime(frames["campaigns"].created_date, utc=True, errors="coerce"),
        pd.Series([pd.Timestamp(ref, tz="UTC")]),
    ]
    allts = pd.concat([pd.to_datetime(s, utc=True, errors="coerce") for s in stamps])
    rng = pd.date_range(allts.min().normalize(), allts.max().normalize(), freq="D", tz="UTC")

    d = pd.DataFrame({"full_date": rng})
    d["date_key"] = d.full_date.dt.strftime("%Y%m%d").astype("Int64")
    d["year"] = d.full_date.dt.year.astype("int32")
    d["quarter"] = d.full_date.dt.year.astype(str) + "Q" + d.full_date.dt.quarter.astype(str)
    d["month"] = d.full_date.dt.month.astype("int32")
    d["day_of_week"] = d.full_date.dt.dayofweek.astype("int32")  # 0=segunda
    d["day_name"] = d.full_date.dt.day_name()
    d["is_weekend"] = d.day_of_week >= 5
    # is_business_day não é enfeite: o BCE só publica câmbio em dia útil, e é
    # esta coluna que explica por que ~32% das transações em moeda estrangeira
    # caem num dia sem cotação própria e usam a última disponível.
    d["is_business_day"] = ~d.is_weekend
    d["full_date"] = d.full_date.dt.date.astype(str)
    return d[CONTRACTS["dim_date"]["columns"]]


def build_dim_player(players):
    d = players.copy()
    d["signup_date_key"] = _date_key(d.signup_date)
    return d[CONTRACTS["dim_player"]["columns"]]


def build_dim_campaign(campaigns):
    d = campaigns.copy()
    d["created_date_key"] = _date_key(d.created_date)
    # 'channel' da campanha é o canal DECLARADO no nome. O canal de verdade
    # está em fact_touchpoint.delivery_channel, que vem do event stream.
    # Nomes iguais para coisas diferentes é como se produz relatório errado.
    d = d.rename(columns={"channel": "channel_declarado"})
    # Relatório de conformidade da taxonomia numa coluna: o nome cru bate com o
    # nome reescrito no padrão? Se não, a campanha violou a convenção.
    d["is_taxonomy_compliant"] = d.campaign_name.fillna("") == d.campaign_name_std
    return d[CONTRACTS["dim_campaign"]["columns"]]


# =============================================================================
# FATOS — grão idêntico ao da silver, só reancorado nas dimensões
# =============================================================================
def build_fact_deposit(deposits):
    f = deposits.copy()
    f["date_key"] = _date_key(f.deposit_ts)
    return f[CONTRACTS["fact_deposit"]["columns"]]


def build_fact_bet(bets):
    f = bets.copy()
    f["date_key"] = _date_key(f.bet_ts)
    # net_brl = stake - payout = GGR. Aditiva e pré-calculada porque somar duas
    # colunas em query é onde nasce o erro de somar stake de um join e payout
    # de outro. Ver athena_gold.sql: NÃO é a medida de valor do jogador.
    f["net_brl"] = (f.stake_brl - f.payout_brl).round(2)
    return f[CONTRACTS["fact_bet"]["columns"]]


def build_fact_touchpoint(touchpoints):
    """
    Fato SEM MEDIDA (factless fact table): um toque não tem valor monetário,
    o que se mede é a ocorrência. As três flags 0/1 existem para que a query
    seja sum(is_click) em vez de count(CASE WHEN ...) — some direto, e funciona
    igual em qualquer nível de agregação.
    """
    f = touchpoints.copy()
    f["date_key"] = _date_key(f.event_ts)
    f = f.rename(columns={"channel": "delivery_channel"})
    for ev in ("sent", "open", "click"):
        f[f"is_{ev}"] = (f.event_type == ev).astype("int32")
    return f[CONTRACTS["fact_touchpoint"]["columns"]]


# =============================================================================
def build_star(src, ref):
    """silver (5 tabelas) -> gold (3 dimensões + 3 fatos)."""
    return {
        "dim_date": build_dim_date(src, ref),
        "dim_player": build_dim_player(src["players"]),
        "dim_campaign": build_dim_campaign(src["campaigns"]),
        "fact_deposit": build_fact_deposit(src["deposits"]),
        "fact_bet": build_fact_bet(src["bets"]),
        "fact_touchpoint": build_fact_touchpoint(src["campaign_touchpoints"]),
    }


def validate(star, src, ref):
    """Contrato por tabela + integridade referencial + invariantes do star."""
    checks = []
    for name in WRITE_ORDER:
        checks += dq.run(name, star[name], CONTRACTS[name], ref)
    # Integridade referencial só faz sentido com todas as tabelas em memória:
    # é aqui que se pega o fato que aponta para uma dimensão inexistente.
    checks += dq.run_foreign_keys(star, CONTRACTS)

    # --- invariantes que nenhum contrato genérico pega ----------------------
    # 1. Nenhum jogador some entre silver e gold. A gold reancora, não filtra.
    n_src = len(src["players"])
    checks.append(dq.expect("dim_player", "no_player_lost", "player_id", ERROR,
                            len(star["dim_player"]) == n_src,
                            abs(len(star["dim_player"]) - n_src), n_src,
                            f"silver={n_src} gold={len(star['dim_player'])}"))
    # 2. Fato preserva a contagem de linhas da silver. Se um fato perder linha
    #    aqui, o dinheiro simplesmente desaparece do relatório.
    for fato, origem in (("fact_deposit", "deposits"), ("fact_bet", "bets"),
                         ("fact_touchpoint", "campaign_touchpoints")):
        n = len(src[origem])
        checks.append(dq.expect(fato, "grain_preserved", "row_count", ERROR,
                                len(star[fato]) == n, abs(len(star[fato]) - n), n,
                                f"silver={n} gold={len(star[fato])}"))
    # 3. O dinheiro atravessa a camada intacto. Contagem de linhas não pega
    #    tudo: um merge mal feito pode manter o número de linhas e alterar
    #    valor. Aqui o número "plausível" não passa.
    for fato, origem, col in (("fact_deposit", "deposits", "amount_brl"),
                              ("fact_bet", "bets", "stake_brl")):
        a = float(pd.to_numeric(src[origem][col], errors="coerce").fillna(0).sum())
        b = float(pd.to_numeric(star[fato][col], errors="coerce").fillna(0).sum())
        checks.append(dq.expect(fato, "sum_preserved", col, ERROR,
                                abs(a - b) < 1.0, int(abs(a - b)), len(star[fato]),
                                f"silver={a:.2f} gold={b:.2f}"))
    return checks


def lambda_handler(event, context):
    event = event or {}
    t0 = time.perf_counter()
    ref = event.get("reference_date") or REFERENCE_DATE
    run_id = getattr(context, "aws_request_id", None) or f"local-{int(time.time())}"

    src = {n: s3_io.read_parquet(BUCKET, f"silver/{n}/{n}.parquet")
           for n in ("players", "deposits", "bets", "campaign_touchpoints", "campaigns")}
    logger.info("silver carregada | " + " | ".join(f"{k}={len(v)}" for k, v in src.items()))

    star = build_star(src, ref)
    checks = validate(star, src, ref)
    logger.log_checks(checks)
    errors = dq.errors(checks)

    # Publicação ATÔMICA em relação ao DQ: ou o star inteiro vai, ou nada vai.
    # Gravar metade das tabelas deixaria o Athena com fato apontando para
    # dimensão de outra execução — pior que não gravar nada.
    written, total_bytes = {}, 0
    if not (errors and FAIL_ON_ERROR):
        for name in WRITE_ORDER:
            key = f"gold/{name}/{name}.parquet"
            total_bytes += s3_io.write_parquet(star[name], BUCKET, key)
            written[name] = len(star[name])

    duration_ms = int((time.perf_counter() - t0) * 1000)
    confirmed_brl = round(float(star["fact_deposit"].loc[
        star["fact_deposit"].is_confirmed, "amount_brl"].sum()), 2)

    logger.emf(
        metrics={"Tables": len(WRITE_ORDER), "Players": len(star["dim_player"]),
                 "ConfirmedDepositBrl": confirmed_brl,
                 "BytesWritten": total_bytes, "ProcessingDurationMs": duration_ms,
                 **dq.metrics_from(checks)},
        dimensions={"Entity": "star", "Layer": "gold"},
        dimension_sets=[["Entity", "Layer"], ["Layer"]],
        properties={"run_id": run_id, "reference_date": ref, "rows": written},
        units={"BytesWritten": "Bytes", "ProcessingDurationMs": "Milliseconds"},
    )
    logger.info("star gold | %s | R$ %.2f confirmados | erros=%d avisos=%d",
                " ".join(f"{k}={len(v)}" for k, v in star.items()),
                confirmed_brl, len(errors), len(dq.warnings(checks)))

    report_key = f"quality/dq_report/ingest_date={ref}/{run_id}-gold.jsonl"
    try:
        s3_io.write_jsonl([{**c.as_dict(), "ingest_date": ref, "run_id": run_id,
                            "checked_at": datetime.now(timezone.utc).isoformat()}
                           for c in checks], BUCKET, report_key)
    except Exception:  # noqa: BLE001 — observabilidade não derruba a carga
        logger.exception("falha ao gravar relatorio de DQ (nao bloqueante)")
        report_key = None

    summary = {
        "run_id": run_id, "reference_date": ref,
        "tables": {k: len(v) for k, v in star.items()},
        "written": written,
        "confirmed_deposit_brl": confirmed_brl,
        "bytes_written": total_bytes,
        "duration_ms": duration_ms,
        "dq": {"checks_run": len(checks), "errors": len(errors),
               "warnings": len(dq.warnings(checks)),
               "report": f"s3://{BUCKET}/{report_key}" if report_key else None,
               "error_details": [c.as_dict() for c in errors[:10]]},
    }

    if errors and FAIL_ON_ERROR:
        raise RuntimeError(
            f"Data Quality reprovou a gold: {len(errors)} violacao(oes) ERROR. "
            f"NADA foi publicado — o star vai inteiro ou nao vai. "
            f"{[(c.entity, c.check, c.column) for c in errors[:5]]}")

    return summary
