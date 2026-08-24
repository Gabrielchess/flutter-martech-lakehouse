"""
I/O no lakehouse. Só o que as três Lambdas repetiriam se isso não existisse.
"""

import io
import json

import boto3
import pandas as pd

_s3 = boto3.client("s3")

NoSuchKey = _s3.exceptions.NoSuchKey


def read_csv(bucket, key, as_text=True) -> pd.DataFrame:
    """
    as_text=True lê tudo como string. Em bronze->silver isso é obrigatório:
    se o pandas inferisse, 'amount' viraria float64 (ponto flutuante para
    dinheiro) e silver já sairia diferente de bronze — o oposto de uma cópia.
    """
    body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_csv(io.BytesIO(body), **({"dtype": str} if as_text else {}))


def read_parquet(bucket, key) -> pd.DataFrame:
    body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def write_parquet(df: pd.DataFrame, bucket, key) -> int:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    payload = buf.getvalue()
    _s3.put_object(Bucket=bucket, Key=key, Body=payload)
    return len(payload)


def write_jsonl(records, bucket, key) -> int:
    """JSONL particionado: o Glue cataloga e o Athena consulta direto."""
    body = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
    _s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    return len(body)
