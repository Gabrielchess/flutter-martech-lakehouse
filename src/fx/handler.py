"""
frankfurter -> reference/fx_rates | Lambda de câmbio do pipeline Martech

Ingere as cotações do BCE via Frankfurter v1 e materializa uma tabela DIÁRIA
DENSA de conversão para BRL.

POR QUE UMA LAMBDA SEPARADA
---------------------------
A API é problema de INGESTÃO, não de transformação. Se a Lambda que converte
chamasse a rede, uma instabilidade derrubaria a carga, e reprocessar março em
junho poderia dar número diferente. Aqui a cotação vira dado no S3; a
transformação lê do S3 e nunca toca na rede.

POR QUE A TABELA É DENSA
------------------------
O BCE só publica em dia útil. No período do case são 244 dias corridos para
174 dias úteis, e ~32% das transações em moeda estrangeira caem em dia sem
cotação. Um join direto com a resposta da API descartaria essas linhas EM
SILÊNCIO. A tabela tem uma linha por dia-corrido x par, com carry-forward
explícito, então o join vira igualdade simples e não perde nada.

Runtime: Python 3.14
Layers:  AWSSDKPandas-Python314 (v11)   # urllib é stdlib, sem dependência nova
Env:     LAKEHOUSE_BUCKET=flutter-martech-lakehouse   (obrigatória)
         FX_START_DATE=2023-07-25   (opcional, default do backfill)
         FX_QUOTE_CURRENCIES=USD,EUR
         DQ_REFERENCE_DATE=2024-04-01
Evento:  {"start": "2023-07-25", "end": "2024-03-31"}   (ambos opcionais)
"""

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from shared import data_quality as dq
from shared import logger, s3_io
from shared.data_quality import ERROR, WARN

BUCKET = os.environ["LAKEHOUSE_BUCKET"]
BASE_URL = "https://api.frankfurter.dev/v1"
BASE = "EUR"                                      # base de publicação do BCE
TARGET = "BRL"                                    # moeda de reporte
QUOTES = [c.strip() for c in os.environ.get("FX_QUOTE_CURRENCIES", "USD,EUR").split(",")]
START_DEFAULT = os.environ.get("FX_START_DATE", "2023-07-25")
FAIL_ON_ERROR = os.environ.get("DQ_FAIL_ON_ERROR", "true").lower() == "true"

CURRENT_KEY = "reference/fx_rates/current/rates.parquet"

# O BCE não publica no dia da transação mais recente se ela for hoje antes das
# 16h CET, e um feriado no início da janela deixaria o carry-forward sem fonte.
# Buscamos com folga para trás e recortamos depois.
LOOKBACK_DAYS = 10

# Faixa de sanidade da taxa para BRL. Cobre a paridade 1.0 (BRL->BRL) e as
# cotações do período (~5,0 a ~5,6). Pega o erro que realmente acontece:
# par invertido (1/5,4 = 0,185) ou resposta corrompida.
RATE_FLOOR, RATE_CEILING = 0.5, 20.0

FX_CONTRACT = {
    "columns": ["rate_date", "from_currency", "to_currency", "rate",
                "source_rate_date", "is_carried_forward", "rate_source", "fetched_at"],
    "not_blank": {
        "rate_date": (ERROR, 0.0),
        "from_currency": (ERROR, 0.0),
        "to_currency": (ERROR, 0.0),
        "rate": (ERROR, 0.0),
    },
    "domains": {
        "from_currency": (ERROR, {"BRL", "USD", "EUR"}),
        "to_currency": (ERROR, {"BRL"}),
        "rate_source": (WARN, {"frankfurter_ecb"}),
    },
    "positive": ["rate"],
    "min_value": {"rate": (ERROR, RATE_FLOOR)},
    "max_value": {"rate": (ERROR, RATE_CEILING)},
    "business_key": ["rate_date", "from_currency", "to_currency"],
}


# =============================================================================
# API
# =============================================================================

def _fetch(url, attempts=4, timeout=15):
    """
    Backoff exponencial. A Frankfurter é gratuita e sem SLA — tratar
    instabilidade como esperada, não como excepcional.
    """
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "flutter-martech-fx/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                raw = resp.read()
                return json.loads(raw), hashlib.sha256(raw).hexdigest()
        except (urllib.error.URLError, TimeoutError, RuntimeError, ValueError) as exc:
            last = exc
            wait = 2 ** i
            logger.info("frankfurter tentativa %d/%d falhou (%s), aguardando %ds",
                        i + 1, attempts, exc, wait)
            if i < attempts - 1:
                time.sleep(wait)
    raise RuntimeError(f"frankfurter indisponivel apos {attempts} tentativas: {last}")


def fetch_range(start: date, end: date):
    """
    Uma requisição cobre o histórico inteiro. Frankfurter aceita intervalo.

    A base NÃO entra em `symbols`: a API não a devolve dentro de `rates`
    (ela vale 1 por definição). Pedir mesmo assim é ruído, e tratar a
    ausência como "sem cotação" derruba o par EUR->BRL inteiro.
    """
    symbols = sorted({c for c in QUOTES + [TARGET] if c != BASE})
    url = f"{BASE_URL}/{start.isoformat()}..{end.isoformat()}?base={BASE}&symbols={','.join(symbols)}"
    logger.info("GET %s", url)
    payload, digest = _fetch(url)
    return payload.get("rates", {}), digest, url


# =============================================================================
# Construção da tabela densa
# =============================================================================

def build_dense(rates: dict, start: date, end: date, fetched_at: str) -> pd.DataFrame:
    """
    Resposta esparsa (só dia útil) -> tabela densa (todo dia corrido).

    Carry-forward, nunca interpolação nem próxima cotação: interpolar cria uma
    taxa que nunca existiu, e usar a próxima seria informação do futuro — as
    duas quebram auditoria e reprodutibilidade.
    """
    calendar = pd.date_range(start, end, freq="D")
    wide = pd.DataFrame(index=calendar)

    for cur in {c for c in QUOTES + [TARGET] if c != BASE}:
        wide[cur] = pd.Series({pd.Timestamp(d): r.get(cur) for d, r in rates.items()})

    # A base não vem na resposta — ela é 1 por definição. Sem esta linha, o par
    # BASE->BRL seria descartado como "sem cotação" e a cobertura acusaria.
    wide[BASE] = 1.0

    # de onde veio a taxa: o dia em que o BCE publicou, propagado adiante
    wide["source_rate_date"] = pd.Series(
        {pd.Timestamp(d): pd.Timestamp(d) for d in rates}, dtype="datetime64[ns]"
    ).reindex(calendar)
    wide = wide.ffill()

    rows = []
    for ts, r in wide.iterrows():
        brl_per_eur = r.get(TARGET)
        if pd.isna(brl_per_eur) or pd.isna(r["source_rate_date"]):
            continue  # antes da primeira publicação disponível — checado por cobertura
        src = r["source_rate_date"].date()
        carried = src != ts.date()

        # Identidade: exata, sem passar por EUR. São 787 depósitos e 2.557
        # apostas já em BRL — arredondar duas vezes ali seria criar erro do nada.
        # E a fonte é o próprio dia: a paridade não depende de publicação do BCE,
        # então marcá-la com a data de sexta-feira seria mentira no rastro.
        rows.append((ts.date(), TARGET, TARGET, 1.0, ts.date(), False))

        for cur in QUOTES:
            per_eur = r.get(cur)
            if pd.isna(per_eur) or per_eur == 0:
                continue
            # cruzada derivada da base do BCE: (BRL/EUR) / (X/EUR).
            # Para cur == BASE, per_eur é 1.0 e a fórmula devolve a cotação direta.
            rate = brl_per_eur / per_eur
            rows.append((ts.date(), cur, TARGET, round(float(rate), 8), src, carried))

    df = pd.DataFrame(rows, columns=["rate_date", "from_currency", "to_currency",
                                     "rate", "source_rate_date", "is_carried_forward"])
    df["rate_source"] = "frankfurter_ecb"
    df["fetched_at"] = fetched_at
    df["rate_date"] = df.rate_date.astype(str)
    df["source_rate_date"] = df.source_rate_date.astype(str)
    return df.sort_values(["rate_date", "from_currency"]).reset_index(drop=True)


def coverage_check(df: pd.DataFrame, start: date, end: date):
    """
    O check que garante que nenhuma transação vai se perder no join: todo dia
    corrido precisa de linha para todo par. Tolerância zero — um buraco aqui
    vira linha descartada em silêncio na conversão.
    """
    expected_days = len(pd.date_range(start, end, freq="D"))
    pairs = len(QUOTES) + 1
    expected = expected_days * pairs
    missing = expected - len(df)
    return dq.expect("fx_rates", "calendar_coverage", "rate_date", ERROR,
                     missing == 0, abs(missing), expected,
                     f"esperado={expected} ({expected_days} dias x {pairs} pares) "
                     f"obtido={len(df)} faltando={missing}")


def jump_check(df: pd.DataFrame):
    """Salto diário > 10% é possível, mas merece olho humano antes de virar receita."""
    bad = 0
    worst = 0.0
    for cur in QUOTES:
        s = df[df.from_currency == cur].sort_values("rate_date").rate.astype(float)
        pct = s.pct_change().abs().dropna()
        bad += int((pct > 0.10).sum())
        worst = max(worst, float(pct.max()) if len(pct) else 0.0)
    return dq.expect("fx_rates", "daily_jump", "rate", WARN, bad == 0, bad, len(df),
                     f"saltos_diarios_acima_de_10pct={bad} maior_salto={worst:.2%}")


# =============================================================================
# HANDLER
# =============================================================================

def lambda_handler(event, context):
    event = event or {}
    t0 = time.perf_counter()
    run_id = getattr(context, "aws_request_id", None) or f"local-{int(time.time())}"
    fetched_at = datetime.now(timezone.utc).isoformat()

    start = date.fromisoformat(event.get("start") or START_DEFAULT)
    end = date.fromisoformat(event["end"]) if event.get("end") else datetime.now(timezone.utc).date()
    if start > end:
        raise ValueError(f"start ({start}) posterior a end ({end})")

    # busca com folga para trás para o carry-forward ter fonte no início da janela
    fetch_start = start - timedelta(days=LOOKBACK_DAYS)

    digest, url, degraded = None, None, False
    try:
        rates, digest, url = fetch_range(fetch_start, end)
        df = build_dense(rates, start, end, fetched_at)
        published_days = len(rates)
    except Exception as exc:  # noqa: BLE001
        # Degradação controlada: cotação de data PASSADA não muda. Se já existe
        # um snapshot que cobre a janela pedida, seguir com ele é melhor que
        # derrubar a carga do dia por indisponibilidade de terceiro.
        logger.exception("frankfurter indisponivel, tentando snapshot anterior")
        try:
            prev = s3_io.read_parquet(BUCKET, CURRENT_KEY)
        except Exception:  # noqa: BLE001
            raise RuntimeError(f"frankfurter indisponivel e sem snapshot anterior: {exc}")
        if prev.rate_date.max() < end.isoformat():
            raise RuntimeError(
                f"frankfurter indisponivel e snapshot anterior so cobre ate "
                f"{prev.rate_date.max()}, insuficiente para {end}: {exc}")
        df = prev[(prev.rate_date >= start.isoformat()) & (prev.rate_date <= end.isoformat())]
        published_days, degraded = 0, True
        logger.info("MODO DEGRADADO | usando snapshot de %s", prev.fetched_at.max())

    # --- qualidade da própria referência ------------------------------------
    checks = dq.run("fx_rates", df, FX_CONTRACT, os.environ.get("DQ_REFERENCE_DATE", "2024-04-01"))
    checks.append(coverage_check(df, start, end))
    checks.append(jump_check(df))
    logger.log_checks(checks)

    carried = int(df.is_carried_forward.sum())
    errors = dq.errors(checks)

    # --- publicar ------------------------------------------------------------
    # Snapshot imutável por dia de coleta: o BCE revisa cotações, e sobrescrever
    # o histórico faria os números do mês passado mudarem sozinhos. O ponteiro
    # 'current' é só a conveniência que a gold lê.
    snapshot_key = f"reference/fx_rates/snapshot_date={date.today().isoformat()}/{run_id}.parquet"
    size = 0
    if not errors or not FAIL_ON_ERROR:
        size = s3_io.write_parquet(df, BUCKET, snapshot_key)
        s3_io.write_parquet(df, BUCKET, CURRENT_KEY)

    duration_ms = int((time.perf_counter() - t0) * 1000)
    logger.emf(
        metrics={"RateRows": len(df), "CalendarDays": len(pd.date_range(start, end, freq="D")),
                 "PublishedDays": published_days, "CarriedForwardRows": carried,
                 "DegradedMode": int(degraded), "BytesWritten": size,
                 "ProcessingDurationMs": duration_ms, **dq.metrics_from(checks)},
        dimensions={"Entity": "fx_rates", "Layer": "reference"},
        dimension_sets=[["Entity", "Layer"], ["Layer"]],
        properties={"run_id": run_id, "start": str(start), "end": str(end),
                    "payload_sha256": digest, "source_url": url},
        units={"BytesWritten": "Bytes", "ProcessingDurationMs": "Milliseconds"},
    )
    logger.info("fx_rates | %d linhas | %d dias corridos | %d publicados pelo BCE | "
                "%d carry-forward (%.0f%%) | erros=%d avisos=%d%s",
                len(df), len(pd.date_range(start, end, freq="D")), published_days, carried,
                100 * carried / max(len(df), 1), len(errors), len(dq.warnings(checks)),
                " | DEGRADADO" if degraded else "")

    report_key = f"quality/dq_report/ingest_date={date.today().isoformat()}/{run_id}-fx.jsonl"
    try:
        s3_io.write_jsonl([{**c.as_dict(), "ingest_date": date.today().isoformat(),
                            "run_id": run_id, "checked_at": fetched_at} for c in checks],
                          BUCKET, report_key)
    except Exception:  # noqa: BLE001 — observabilidade não derruba a carga
        logger.exception("falha ao gravar relatorio de DQ (nao bloqueante)")
        report_key = None

    summary = {
        "run_id": run_id, "start": str(start), "end": str(end),
        "rows": len(df), "published_days": published_days,
        "carried_forward": carried, "degraded_mode": degraded,
        "payload_sha256": digest, "duration_ms": duration_ms,
        "snapshot": f"s3://{BUCKET}/{snapshot_key}" if size else None,
        "current": f"s3://{BUCKET}/{CURRENT_KEY}" if size else None,
        "dq": {"checks_run": len(checks), "errors": len(errors),
               "warnings": len(dq.warnings(checks)),
               "report": f"s3://{BUCKET}/{report_key}" if report_key else None,
               "error_details": [c.as_dict() for c in errors[:10]]},
    }

    if errors and FAIL_ON_ERROR:
        raise RuntimeError(
            f"Data Quality reprovou a tabela de cambio: {len(errors)} violacao(oes) ERROR. "
            f"Nada foi publicado. {[(c.check, c.column) for c in errors[:5]]}")

    return summary
