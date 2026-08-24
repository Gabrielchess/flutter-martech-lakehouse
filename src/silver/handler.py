"""
bronze -> silver | Lambda 1 do pipeline Martech (Flutter Brazil)

Lê os 5 CSVs de bronze/, valida contra o contrato, deduplica, quarentena o que
não passa, TIPA as colunas e CONVERTE os valores para BRL — gravando silver/
pronta para análise.

Silver é a camada LIMPA: sai daqui sem linha repetida, com tipo de verdade e
com valor em BRL ao lado do original. Fica de fora o que é semântica de
negócio: parse da taxonomia de campanha, régua de dormência e agregações por
jogador são trabalho da gold.

DEPENDÊNCIA: precisa de reference/fx_rates/current/rates.parquet, produzido
pela lambda flutter-fx. No Step Functions ela roda ANTES desta.

Runtime: Python 3.14
Layers:  AWSSDKPandas-Python314 (v11) + flutter-shared
Env:     LAKEHOUSE_BUCKET=flutter-martech-lakehouse   (obrigatória)
         DQ_REFERENCE_DATE=2024-04-01   (opcional) corte de data futura
         DQ_FAIL_ON_ERROR=true          (opcional)
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd

from shared import data_quality as dq
from shared import logger, s3_io, taxonomy, transform
from shared.data_quality import ERROR, WARN

BUCKET = os.environ["LAKEHOUSE_BUCKET"]
REFERENCE_DATE = os.environ.get("DQ_REFERENCE_DATE", "2024-04-01")
FAIL_ON_ERROR = os.environ.get("DQ_FAIL_ON_ERROR", "true").lower() == "true"
FX_KEY = "reference/fx_rates/current/rates.parquet"

# =============================================================================
# CONTRATO DA CAMADA BRONZE
# =============================================================================
# As tolerâncias saíram do profiling da carga inicial, não de chute. Onde a
# imperfeição é conhecida e será tratada adiante, o check é WARN com folga
# sobre o valor observado; onde o dado hoje está limpo, é ERROR com tolerância
# zero. É isso que faz o alarme significar "algo mudou" em vez de "esse
# dataset é sujo". Formato do contrato: ver docstring de shared/data_quality.

CONTRACTS = {
    "players": {
        "columns": ["player_id", "signup_date", "acquisition_channel", "country",
                    "preferred_currency", "kyc_status", "self_excluded"],
        "expected_rows": 250,
        "pk": "player_id",
        "not_blank": {
            "player_id": (ERROR, 0.0),
            "country": (ERROR, 0.0),
            "preferred_currency": (ERROR, 0.0),
            "acquisition_channel": (WARN, 0.15),   # 22/250 = 8.8%, vira 'unknown' na gold
        },
        "domains": {
            "country": (WARN, {"BR", "PT", "AO"}),          # valor novo = mercado novo
            "preferred_currency": (ERROR, {"BRL", "USD", "EUR"}),
            "kyc_status": (WARN, {"verified", "pending", "rejected"}),
            "self_excluded": (ERROR, {"true", "false"}),
        },
        "dates": ["signup_date"],
        "pattern": {"player_id": r"P\d{5}"},
        "no_spaces": ["player_id"],
    },
    "deposits": {
        "columns": ["deposit_id", "player_id", "deposit_ts", "amount", "currency", "status"],
        "expected_rows": 1176,
        "pk": "deposit_id",
        "business_key": ["player_id", "deposit_ts", "amount", "currency", "status"],
        "not_blank": {
            "deposit_id": (ERROR, 0.0), "player_id": (ERROR, 0.0),
            "amount": (ERROR, 0.0), "currency": (ERROR, 0.0), "status": (ERROR, 0.0),
        },
        "domains": {
            "currency": (ERROR, {"BRL", "USD", "EUR"}),     # moeda nova quebra o câmbio
            "status": (ERROR, {"confirmed", "pending", "failed"}),
        },
        "dates": ["deposit_ts"],
        "positive": ["amount"],
        "max_value": {"amount": (WARN, 50_000.0)},   # observado: max 1.997. Teto pega erro de unidade.
        "pattern": {"deposit_id": r"D\d{7}", "player_id": r"P\d{5}"},
        "no_spaces": ["deposit_id", "player_id"],
        "fks": {"player_id": "players.player_id"},
    },
    "bets": {
        "columns": ["bet_id", "player_id", "bet_ts", "stake", "currency", "product", "payout"],
        "expected_rows": 3702,
        "pk": "bet_id",
        "business_key": ["player_id", "bet_ts", "stake", "currency", "product", "payout"],
        "not_blank": {
            "bet_id": (ERROR, 0.0), "player_id": (ERROR, 0.0), "stake": (ERROR, 0.0),
            "payout": (ERROR, 0.0), "currency": (ERROR, 0.0),
        },
        "domains": {
            "currency": (ERROR, {"BRL", "USD", "EUR"}),
            "product": (ERROR, {"sports", "casino"}),
        },
        "dates": ["bet_ts"],
        "positive": ["stake"],
        "non_negative": ["payout"],   # payout=0 é aposta perdida (2080 linhas), não dado faltante
        "max_value": {"stake": (WARN, 50_000.0), "payout": (WARN, 500_000.0)},
        "pattern": {"bet_id": r"B\d{7}", "player_id": r"P\d{5}"},
        "no_spaces": ["bet_id", "player_id"],
        "fks": {"player_id": "players.player_id"},
    },
    "campaigns": {
        "columns": ["campaign_id", "campaign_name", "created_date", "status"],
        "expected_rows": 12,
        "pk": "campaign_id",
        "not_blank": {
            "campaign_id": (ERROR, 0.0),
            "campaign_name": (WARN, 0.20),         # C007 vazio (1/12), vira offer='unknown'
        },
        "domains": {"status": (WARN, {"active", "paused", "archived"})},
        "dates": ["created_date"],
        "pattern": {"campaign_id": r"C\d{3}"},
        "no_spaces": ["campaign_id"],
    },
    "campaign_touchpoints": {
        "columns": ["touchpoint_id", "player_id", "campaign_id", "channel", "event_ts", "event_type"],
        "expected_rows": 1260,
        "pk": "touchpoint_id",
        "business_key": ["player_id", "campaign_id", "channel", "event_ts", "event_type"],
        "not_blank": {
            "touchpoint_id": (ERROR, 0.0), "player_id": (ERROR, 0.0), "campaign_id": (ERROR, 0.0),
        },
        "domains": {
            "channel": (ERROR, {"email", "push", "sms"}),
            "event_type": (ERROR, {"sent", "open", "click"}),
        },
        "dates": ["event_ts"],       # 2 eventos passam de 2024-04-01; a gold corta pela referência
        "pattern": {"touchpoint_id": r"T\d{7}", "player_id": r"P\d{5}", "campaign_id": r"C\d{3}"},
        "no_spaces": ["touchpoint_id", "player_id", "campaign_id"],
        "fks": {"player_id": "players.player_id", "campaign_id": "campaigns.campaign_id"},
    },
}

# =============================================================================
# TRANSFORMAÇÕES DA CAMADA SILVER
# =============================================================================
# O contrato acima valida o texto CRU que chega de bronze. Este bloco descreve
# o que vira dado tipado e convertido. Separados de propósito: o contrato
# responde "o que chegou está aceitável?", isto responde "no que vira?".

TRANSFORMS = {
    "players": {
        "dates": ["signup_date"],
        "booleans": ["self_excluded"],
        # 22 jogadores (8,8%) sem canal. Viram 'unknown' e continuam na base —
        # descartá-los tiraria quase um décimo do LTV por canal em silêncio.
        "fill": {"acquisition_channel": "unknown"},
    },
    "deposits": {
        "timestamps": ["deposit_ts"],
        "numbers": ["amount"],
        "money": {"cols": ["amount"], "currency": "currency", "date": "deposit_ts"},
    },
    "bets": {
        "timestamps": ["bet_ts"],
        "numbers": ["stake", "payout"],
        "money": {"cols": ["stake", "payout"], "currency": "currency", "date": "bet_ts"},
    },
    "campaigns": {"dates": ["created_date"]},
    "campaign_touchpoints": {"timestamps": ["event_ts"]},
}


# Vocabulário controlado da taxonomia de campanha (seção 2b do case). É regra
# de negócio, não dado observável — por isso vive declarada aqui.
TAXONOMY = {
    "geo": ["BR", "PT", "AO"],
    "channel": ["email", "push", "sms"],
    "objective": ["acquisition", "reactivation", "retention", "crosssell"],
    "product": ["sports", "casino", "both"],
    "audience": ["new", "active", "dormant", "vip"],
    "offer": ["bonus50", "bonus100", "freebet", "freespins", "cashback", "none"],
}


def derive(entity, df):
    """
    Colunas calculadas que a análise vai precisar e que ninguém deveria
    recalcular a cada consulta.
    """
    if entity == "campaigns":
        # 7 segmentos + o nome reescrito no padrão. Sem colunas de metadado do
        # parse: conformidade é campaign_name_std == campaign_name, e contagem
        # de segmentos é contar quantos não são 'unknown'. Ambas deriváveis.
        parsed = df.campaign_name.map(lambda n: taxonomy.parse(n, TAXONOMY))
        for seg in taxonomy.ORDER:
            df[seg] = parsed.map(lambda p, s=seg: p[s])
        df["campaign_name_std"] = parsed.map(taxonomy.standard_name)

    elif entity == "players":
        # Porta de compliance: 14 auto-excluídos + 12 com KYC rejeitado ficam
        # fora de qualquer lista de campanha. Marcar aqui, uma vez, evita que
        # alguém esqueça o filtro numa query.
        df["is_targetable"] = (~df.self_excluded.fillna(False)) & (df.kyc_status != "rejected")

    elif entity == "deposits":
        # Só 'confirmed' é dinheiro de verdade (950 de 1151). 'failed' e
        # 'pending' ficam na base — a falha de pagamento é sinal de intenção,
        # e sumir com ela apagaria um microssegmento valioso.
        df["is_confirmed"] = df.status == "confirmed"

    return df


def quarantine_reasons(df, contract):
    """
    Decide, linha a linha, o que NÃO entra em silver — e por quê.

    Devolve uma Series de motivos (NaN = a linha segue). Uma linha recebe
    apenas o PRIMEIRO motivo que casa, senão ela seria contada duas vezes na
    quarentena e o invariante de reconciliação quebraria.

    Só entram aqui rejeições resolvíveis sem ambiguidade. Caso conflitante
    (mesma chave, conteúdo diferente) não é quarentenado: é ERROR e para o
    pipeline, porque não dá para decidir sozinho qual linha é a verdadeira.
    """
    reasons = pd.Series(pd.NA, index=df.index, dtype="object")

    # 1. linha byte-idêntica: as cópias são indistinguíveis, manter a primeira
    dup = df.duplicated(keep="first")
    reasons[dup & reasons.isna()] = "duplicate_row"

    # 2. evento posterior à data de referência do sistema
    ref = pd.Timestamp(REFERENCE_DATE, tz="UTC")
    for col in contract.get("dates", []):
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True, format="mixed")
        future = (parsed > ref).fillna(False)
        reasons[future & reasons.isna()] = f"future_dated__{col}"

    return reasons


def lambda_handler(event, context):
    event = event or {}
    t0 = time.perf_counter()

    ingest_date = event.get("ingest_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entities = event.get("entities") or list(CONTRACTS)
    run_id = getattr(context, "aws_request_id", None) or f"local-{int(time.time())}"

    frames, loaded, results, failures, checks = {}, [], [], [], []

    # Câmbio vem do S3, nunca da rede: a tabela já foi ingerida e validada pela
    # lambda flutter-fx. Se ela não existe, a conversão não pode acontecer e
    # falhar cedo é melhor que gravar silver sem valor em BRL.
    try:
        fx = s3_io.read_parquet(BUCKET, FX_KEY)
        logger.info("cambio carregado | %d taxas | %s a %s",
                    len(fx), fx.rate_date.min(), fx.rate_date.max())
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"tabela de cambio ausente em s3://{BUCKET}/{FX_KEY}. "
            f"Rode a lambda flutter-fx antes: {exc}")

    # --- 1. ler e validar ----------------------------------------------------
    for entity in entities:
        src = f"bronze/{entity}/ingest_date={ingest_date}/{entity}.csv"
        try:
            df = s3_io.read_csv(BUCKET, src)
            frames[entity] = df
            entity_checks = dq.run(entity, df, CONTRACTS[entity], REFERENCE_DATE)
            checks += entity_checks
            logger.log_checks(entity_checks)
            loaded.append((entity, df, src, entity_checks, time.perf_counter()))
        except s3_io.NoSuchKey:
            msg = f"objeto nao encontrado: s3://{BUCKET}/{src}"
            logger.exception("%s | %s", entity, msg)
            failures.append({"entity": entity, "error": msg})
        except Exception as exc:  # noqa: BLE001 — queremos o resumo completo
            logger.exception("%s | falhou na leitura/validacao", entity)
            failures.append({"entity": entity, "error": f"{type(exc).__name__}: {exc}"})

    # --- 2. integridade referencial (precisa de todas as entidades) ----------
    fk_checks = dq.run_foreign_keys(frames, CONTRACTS)
    checks += fk_checks
    logger.log_checks(fk_checks)

    # --- 3. deduplicar, quarentenar e escrever silver -------------------------
    # Silver é a camada LIMPA: sai daqui sem linha repetida. Só deduplicamos o
    # que é resolvível sem ambiguidade — linha byte-idêntica, onde as cópias são
    # indistinguíveis e escolher qualquer uma dá o mesmo resultado.
    #
    # Chave duplicada com CONTEÚDO diferente nunca é deduplicada aqui: não dá
    # para saber qual linha é a verdadeira. Esse caso é ERROR e para o pipeline.
    #
    # Toda linha removida vai para quarantine/ com o motivo, o que sustenta o
    # invariante: linhas_bronze == linhas_silver + linhas_quarentena.
    # Sem ele, dedup vira perda de dado silenciosa.
    for entity, df, src, entity_checks, t_start in loaded:
        dst = f"silver/{entity}/{entity}.parquet"
        try:
            reasons = quarantine_reasons(df, CONTRACTS[entity])
            clean = df[reasons.isna()]
            removed = df[reasons.notna()]
            quarantined = {}

            for reason, group in df[reasons.notna()].groupby(reasons[reasons.notna()]):
                q_key = (f"quarantine/{entity}/ingest_date={ingest_date}"
                         f"/reason={reason}/{run_id}.parquet")
                s3_io.write_parquet(
                    group.assign(_quarantine_reason=reason,
                                 _run_id=run_id,
                                 _quarantined_at=datetime.now(timezone.utc).isoformat()),
                    BUCKET, q_key)
                quarantined[reason] = {"rows": len(group), "key": f"s3://{BUCKET}/{q_key}"}

            # --- tipagem e conversão -----------------------------------------
            # Só o que sobreviveu à quarentena é transformado. Converter uma
            # linha que vai ser rejeitada é trabalho jogado fora, e pior:
            # colocaria taxa de câmbio em registro descartado, sugerindo que
            # ele foi processado.
            spec = TRANSFORMS.get(entity, {})
            typed = transform.cast(clean, spec)

            money = spec.get("money")
            fx_checks = []
            if money:
                typed = transform.to_brl(typed, fx, money["cols"], money["currency"],
                                         money["date"])
                missing = transform.missing_rate_count(typed)
                # Tolerância zero: cada linha sem taxa é receita que sumiria na
                # soma sem levantar exceção nenhuma.
                fx_checks.append(dq.expect(
                    entity, "fx_rate_coverage", money["currency"], ERROR,
                    missing == 0, missing, len(typed),
                    f"linhas_sem_taxa={missing} moedas={sorted(typed[money['currency']].unique())}"))

            typed = derive(entity, typed)
            logger.log_checks(fx_checks)
            checks += fx_checks

            size = s3_io.write_parquet(typed, BUCKET, dst)

            reconciled = len(clean) + len(removed) == len(df)
            all_for_entity = (entity_checks
                              + [c for c in fk_checks if c.entity == entity]
                              + fx_checks
                              + [dq.reconciliation(entity, len(df), len(clean), len(removed))])
            logger.log_checks(all_for_entity[-1:])
            checks.append(all_for_entity[-1])

            logger.emf(
                metrics={"RowCount": len(typed), "RowsIngested": len(df),
                         "RowsQuarantined": len(removed), "ColumnCount": len(typed.columns),
                         "BytesWritten": size,
                         "ProcessingDurationMs": int((time.perf_counter() - t_start) * 1000),
                         **dq.metrics_from(all_for_entity)},
                dimensions={"Entity": entity, "Layer": "silver"},
                # Dois recortes: por entidade (diagnóstico) e agregado — assim um
                # alarme só cobre o pipeline inteiro, sem replicar 5 alarmes iguais.
                dimension_sets=[["Entity", "Layer"], ["Layer"]],
                properties={"ingest_date": ingest_date, "run_id": run_id},
                units={"BytesWritten": "Bytes", "ProcessingDurationMs": "Milliseconds"},
            )
            logger.info("%s | %d lidas -> %d gravadas + %d quarentena | %s -> %s (%d bytes)"
                        " | erros=%d avisos=%d reconcilia=%s",
                        entity, len(df), len(clean), len(removed), src, dst, size,
                        len(dq.errors(all_for_entity)), len(dq.warnings(all_for_entity)),
                        reconciled)
            results.append({"entity": entity, "source": src, "target": dst,
                            "rows_ingested": len(df), "rows_written": len(typed), "columns_out": len(typed.columns),
                            "rows_quarantined": len(removed),
                            "quarantine": quarantined or None,
                            "columns": list(df.columns), "bytes": size})
        except Exception as exc:  # noqa: BLE001
            logger.exception("%s | falhou na escrita", entity)
            failures.append({"entity": entity, "error": f"{type(exc).__name__}: {exc}"})

    # --- 4. publicar ---------------------------------------------------------
    duration_ms = int((time.perf_counter() - t0) * 1000)
    logger.emf(
        metrics={"EntitiesProcessed": len(results), "EntitiesFailed": len(failures),
                 "PipelineChecksFailedError": len(dq.errors(checks)),
                 "PipelineChecksFailedWarn": len(dq.warnings(checks)),
                 "PipelineDurationMs": duration_ms},
        dimensions={"Pipeline": "bronze_to_silver", "Layer": "silver"},
        properties={"ingest_date": ingest_date, "run_id": run_id},
        units={"PipelineDurationMs": "Milliseconds"},
    )

    # O CloudWatch responde "quebrou agora?"; esta tabela responde "como a
    # qualidade evoluiu em 6 meses?" — retenção e custo do CloudWatch estão
    # errados para a segunda pergunta.
    report_key = f"quality/dq_report/ingest_date={ingest_date}/{run_id}.jsonl"
    try:
        checked_at = datetime.now(timezone.utc).isoformat()
        s3_io.write_jsonl(
            [{**c.as_dict(), "ingest_date": ingest_date, "run_id": run_id, "checked_at": checked_at}
             for c in checks], BUCKET, report_key)
        logger.info("relatorio de DQ | s3://%s/%s | %d checks", BUCKET, report_key, len(checks))
    except Exception:  # noqa: BLE001 — observabilidade não derruba a carga
        logger.exception("falha ao gravar relatorio de DQ (nao bloqueante)")
        report_key = None

    dq_errors = dq.errors(checks)
    summary = {
        "ingest_date": ingest_date, "run_id": run_id, "duration_ms": duration_ms,
        "ok": len(results), "failed": len(failures),
        "dq": {"checks_run": len(checks), "errors": len(dq_errors),
               "warnings": len(dq.warnings(checks)),
               "report": f"s3://{BUCKET}/{report_key}" if report_key else None,
               "error_details": [c.as_dict() for c in dq_errors[:20]]},
        "results": results, "failures": failures,
    }

    # Falha explícita para o Catch -> SNS. As entidades que deram certo já foram
    # gravadas, então o retry é seguro (a escrita é overwrite idempotente).
    if failures:
        raise RuntimeError(f"{len(failures)} entidade(s) falharam: {summary}")
    if dq_errors and FAIL_ON_ERROR:
        raise RuntimeError(
            f"Data Quality reprovou: {len(dq_errors)} violacao(oes) ERROR. "
            f"Primeiras: {[(c.entity, c.check, c.column) for c in dq_errors[:5]]}. "
            f"Relatorio: s3://{BUCKET}/{report_key}")

    return summary
