"""
Executa um arquivo .sql inteiro no Athena, statement por statement.

    pip install boto3
    python run_athena.py athena_gold.sql

    python run_athena.py athena_gold.sql athena_silver.sql athena_dq_report.sql

Requer credencial AWS ja configurada (aws configure / variaveis de ambiente).
Para em caso de falha e imprime o erro do Athena.
"""

import sys
import time

import boto3

REGION = "us-east-2"
DATABASE = "flutter_martech"
OUTPUT = "s3://flutter-martech-lakehouse/athena-results/"
WORKGROUP = "primary"

athena = boto3.client("athena", region_name=REGION)


def statements(path):
    sql = open(path, encoding="utf-8").read()
    return [s.strip() for s in sql.split(";") if s.strip()]


def run(sql):
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if r["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return qid, r["State"], r.get("StateChangeReason", "")
        time.sleep(1)


def main(paths):
    for path in paths:
        stmts = statements(path)
        print(f"\n=== {path} | {len(stmts)} statements ===")
        for i, sql in enumerate(stmts, 1):
            head = " ".join(sql.split())[:70]
            qid, state, reason = run(sql)
            print(f"[{i:>2}/{len(stmts)}] {state:<9} {head}")
            if state != "SUCCEEDED":
                print(f"\n  {reason}\n  query id: {qid}")
                sys.exit(1)
    print("\nOK")


if __name__ == "__main__":
    main(sys.argv[1:] or ["athena_gold.sql"])
