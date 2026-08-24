"""
Emissão de métricas e logs estruturados para o CloudWatch.

Usa EMF (Embedded Metric Format): um print de JSON no stdout que o CloudWatch
converte em métrica sozinho. Zero chamada de API dentro da Lambda — sem
latência somada ao billing, sem throttle de PutMetricData, sem custo por
request, e o log bruto continua no CloudWatch Logs para investigação.
"""

import json
import logging
import os
import time

NAMESPACE = os.environ.get("DQ_NAMESPACE", "FlutterMartech/DataQuality")

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def emf(metrics, dimensions, properties=None, dimension_sets=None, units=None):
    """
    Emite um bloco EMF.

    metrics        {nome: valor_numerico}
    dimensions     {nome: valor} — vira série temporal, use baixa cardinalidade
    properties     {nome: valor} — pesquisável no Logs Insights, NÃO vira métrica
    dimension_sets [[...], [...]] — recortes; default é um só, com tudo
    units          {nome_metrica: "Bytes"|"Milliseconds"|...} — default Count
    """
    units = units or {}
    payload = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": NAMESPACE,
                "Dimensions": dimension_sets or [list(dimensions)],
                "Metrics": [{"Name": n, "Unit": units.get(n, "Count")} for n in metrics],
            }],
        },
    }
    payload.update({k: str(v) for k, v in dimensions.items()})   # dimensão = string
    payload.update(metrics)                                      # métrica = número
    if properties:
        # Propriedades ficam pesquisáveis mas não viram dimensão. É de propósito:
        # ingest_date como dimensão criaria uma série nova por dia, e o alarme
        # nunca acumularia histórico.
        payload.update(properties)
    print(json.dumps(payload, default=str))


def log_checks(results):
    """
    Uma linha JSON plana por check COM REGISTROS AFETADOS — tenha ele passado
    ou não.

    Logar só o que reprova era insuficiente: a métrica agregada diz
    "BlankValues=22" mas não diz em QUAL coluna. Como o check de
    `acquisition_channel` passa (8,8% dentro dos 15% tolerados), essa
    informação nunca chegava ao CloudWatch. Agora chega, marcada com
    dq_violation=0 para não poluir a visão de violações.

    dq_check=1     -> tem registros afetados (medição)
    dq_violation=1 -> estourou a tolerância do contrato (veredito)
    """
    for r in results:
        if r.passed and r.failed_records == 0:
            continue
        line = json.dumps(
            {"dq_check": 1, "dq_violation": 0 if r.passed else 1, **r.as_dict()},
            default=str,
        )
        if not r.passed:
            (LOG.error if r.severity == "ERROR" else LOG.warning)(line)
        else:
            LOG.info(line)


def info(msg, *args):
    LOG.info(msg, *args)


def exception(msg, *args):
    LOG.exception(msg, *args)
