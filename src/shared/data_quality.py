"""
Motor de Data Quality.

Recebe um contrato e um DataFrame, devolve resultados. Não corrige nada:
se o motor consertasse o dado, a camada deixaria de ser cópia fiel da anterior
e o reprocesso do zero pararia de ser possível.

O contrato é dado de domínio e vive no handler de cada camada — bronze valida
texto cru ("amount parseia como número?"), silver valida dado tipado
("amount_brl é positivo?"). Mesmo motor, regras diferentes.

FORMATO DO CONTRATO
-------------------
{
  "columns":       [str, ...]                 # schema esperado (ERROR se mudar)
  "expected_rows": int                        # opcional, volume de referência
  "pk":            coluna   # conflito=ERROR, duplicata identica=WARN
  "business_key":  [coluna, ...]  # mesmo evento com ID diferente, ERROR
  "not_blank":     {coluna: (severidade, ratio_max)}
  "domains":       {coluna: (severidade, {valores_aceitos})}
  "dates":         [coluna, ...]              # parse=ERROR, futuro=WARN
  "positive":      [coluna, ...]              # precisa ser > 0
  "non_negative":  [coluna, ...]              # precisa ser >= 0
  "max_value":     {coluna: (severidade, teto)}  # teto plausível de negócio
  "min_value":     {coluna: (severidade, piso)}  # piso plausível de negócio
  "pattern":       {coluna: regex}            # formato do identificador, ERROR
  "no_spaces":     [coluna, ...]              # sem espaço nas pontas, ERROR
  "fks":           {coluna: "entidade.coluna"} # sempre ERROR
}
"""

import re
from dataclasses import dataclass, asdict

import pandas as pd

ERROR = "ERROR"   # quebra o pipeline: o dado está estruturalmente errado
WARN = "WARN"     # registra e segue: imperfeição conhecida, tratada adiante

ROW_DRIFT_RATIO = 0.30   # desvio tolerado no volume antes de avisar


@dataclass
class CheckResult:
    entity: str
    check: str
    column: str
    severity: str
    passed: bool
    failed_records: int
    total_records: int
    details: str = ""

    def as_dict(self):
        d = asdict(self)
        d["failed_ratio"] = round(self.failed_records / self.total_records, 6) if self.total_records else 0.0
        return d


def expect(entity, check, column, severity, passed, failed, total, details=""):
    """
    Constrói um CheckResult avulso. É o que permite um handler declarar um
    check sob medida (cobertura de calendário, reconciliação) sem precisar
    virar regra genérica no contrato.
    """
    return CheckResult(entity, check, column or "", severity, bool(passed),
                       int(failed), int(total), details)


_r = expect  # alias interno


def _blank(s: pd.Series) -> pd.Series:
    """Vazio = NaN OU string só de espaços. O CSV entrega os dois casos."""
    return s.isna() | (s.astype(str).str.strip() == "")


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------

def _schema(entity, df, c):
    expected, actual = c["columns"], list(df.columns)
    missing = [x for x in expected if x not in actual]
    extra = [x for x in actual if x not in expected]
    ok = not missing and not extra
    return _r(entity, "schema_contract", None, ERROR, ok, len(missing) + len(extra),
              len(expected), "" if ok else f"faltando={missing} inesperadas={extra}")


def _rows(entity, df, c):
    n = len(df)
    out = [_r(entity, "row_count_not_zero", None, ERROR, n > 0, 0 if n else 1, max(n, 1), f"linhas={n}")]
    exp = c.get("expected_rows")
    if exp:
        drift = abs(n - exp) / exp
        out.append(_r(entity, "row_count_drift", None, WARN, drift <= ROW_DRIFT_RATIO,
                      abs(n - exp), exp,
                      f"esperado~{exp} recebido={n} desvio={drift:.1%} limite={ROW_DRIFT_RATIO:.0%}"))
    return out


def _pk(entity, df, c):
    """
    Unicidade NÃO leva tolerância percentual. "Até 5% das chaves podem estar
    duplicadas" não é uma regra de negócio — chave primária é propriedade, não
    taxa. O que separa gravidade aqui é a NATUREZA da duplicata:

      - conteúdo conflitante -> ERROR. Duas linhas com a mesma chave e valores
        diferentes: você não sabe qual é a verdadeira. Nenhum dedup automático
        é seguro. Pipeline para.
      - linha byte-idêntica  -> WARN. É reenvio/reprocesso; deduplicar por
        chave é seguro e acontece na gold. Registra e segue.
    """
    col = c.get("pk")
    if not col or col not in df.columns:
        return []
    dup_mask = df[col].duplicated()
    dups = int(dup_mask.sum())

    conflicting_keys = 0
    if dups:
        repeated = df.loc[dup_mask, col].unique()
        sub = df[df[col].isin(repeated)]
        # após remover linhas idênticas, chave que ainda aparece >1 vez conflita
        conflicting_keys = int((sub.drop_duplicates().groupby(col).size() > 1).sum())

    identical = dups - conflicting_keys
    return [
        _r(entity, "primary_key_conflict", col, ERROR, conflicting_keys == 0,
           conflicting_keys, len(df),
           f"chaves_com_conteudo_conflitante={conflicting_keys}"),
        _r(entity, "primary_key_unique", col, WARN, dups == 0, dups, len(df),
           f"duplicados={dups} (linha_identica={identical} conflitantes={conflicting_keys})"),
    ]


def _business_key(entity, df, c):
    """
    O mesmo evento de negócio chegando com IDs substitutos DIFERENTES.

    É o buraco que o check de PK não cobre: as chaves são todas únicas, o
    schema está certo, nada acusa — e o dinheiro é contado duas vezes. Acontece
    quando a origem regenera o ID no reprocessamento.

    Reenvio byte-idêntico é descontado antes (já é reportado por
    primary_key_unique), então o que sobra aqui é só o caso perigoso.
    Tolerância zero: hoje são 0, qualquer ocorrência é regressão.
    """
    bk = c.get("business_key")
    if not bk or not all(k in df.columns for k in bk):
        return []
    n = int(df.drop_duplicates().duplicated(subset=bk).sum())
    return [_r(entity, "duplicate_event", "+".join(bk), ERROR, n == 0, n, len(df),
               f"mesmo_evento_com_id_diferente={n}")]


def _not_blank(entity, df, c):
    out = []
    for col, (sev, max_ratio) in c.get("not_blank", {}).items():
        if col not in df.columns:
            continue
        n = int(_blank(df[col]).sum())
        ratio = n / len(df) if len(df) else 0.0
        out.append(_r(entity, "not_blank", col, sev, ratio <= max_ratio, n, len(df),
                      f"vazios={n} ({ratio:.1%}) limite={max_ratio:.1%}"))
    return out


def _domains(entity, df, c):
    """Valor fora do vocabulário controlado = o sistema fonte mudou."""
    out = []
    for col, (sev, allowed) in c.get("domains", {}).items():
        if col not in df.columns:
            continue
        vals = df.loc[~_blank(df[col]), col].astype(str).str.strip()
        bad = ~vals.isin(allowed)
        n = int(bad.sum())
        out.append(_r(entity, "accepted_values", col, sev, n == 0, n, len(df),
                      f"invalidos={n} inesperados={sorted(vals[bad].unique())[:10]} "
                      f"permitidos={sorted(allowed)}"))
    return out


def _dates(entity, df, c, reference_date):
    """Duas perguntas diferentes: a data parseia, e a data faz sentido no tempo."""
    out = []
    ref = pd.Timestamp(reference_date, tz="UTC")
    for col in c.get("dates", []):
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df.loc[~_blank(df[col]), col], errors="coerce",
                                utc=True, format="mixed")
        bad = int(parsed.isna().sum())
        out.append(_r(entity, "timestamp_parseable", col, ERROR, bad == 0, bad, len(df),
                      f"nao_parseaveis={bad}"))
        valid = parsed.dropna()
        future = int((valid > ref).sum())
        out.append(_r(entity, "not_future_dated", col, WARN, future == 0, future, len(df),
                      f"posteriores_a_{reference_date}={future} "
                      f"max={valid.max() if len(valid) else 'n/a'}"))
    return out


def _numerics(entity, df, c):
    out = []
    for kind, cols in (("positive", c.get("positive", [])),
                       ("non_negative", c.get("non_negative", []))):
        for col in cols:
            if col not in df.columns:
                continue
            parsed = pd.to_numeric(df.loc[~_blank(df[col]), col], errors="coerce")
            bad = int(parsed.isna().sum())
            out.append(_r(entity, "numeric_parseable", col, ERROR, bad == 0, bad, len(df),
                          f"nao_parseaveis={bad}"))
            valid = parsed.dropna()
            n = int((valid <= 0).sum()) if kind == "positive" else int((valid < 0).sum())
            out.append(_r(entity, "numeric_range", col, ERROR, n == 0, n, len(df),
                          f"{kind}: violacoes={n} min={valid.min() if len(valid) else 'n/a'}"))
    return out


def _ceiling(entity, df, c):
    """
    Teto plausível de negócio. `positive` pega o sinal errado; isto pega a
    ordem de grandeza errada — depósito de 10 milhões passa em "> 0" mas é
    quase sempre erro de unidade (centavos lidos como reais) ou fraude.
    """
    out = []
    for col, (sev, ceiling) in c.get("max_value", {}).items():
        if col not in df.columns:
            continue
        valid = pd.to_numeric(df.loc[~_blank(df[col]), col], errors="coerce").dropna()
        n = int((valid > ceiling).sum())
        out.append(_r(entity, "value_ceiling", col, sev, n == 0, n, len(df),
                      f"acima_de_{ceiling}={n} max={valid.max() if len(valid) else 'n/a'}"))
    return out


def _floor(entity, df, c):
    """Piso plausível. Num par de câmbio, pega inversão: BRL/EUR ~5,4 vira 0,185."""
    out = []
    for col, (sev, floor) in c.get("min_value", {}).items():
        if col not in df.columns:
            continue
        valid = pd.to_numeric(df.loc[~_blank(df[col]), col], errors="coerce").dropna()
        n = int((valid < floor).sum())
        out.append(_r(entity, "value_floor", col, sev, n == 0, n, len(df),
                      f"abaixo_de_{floor}={n} min={valid.min() if len(valid) else 'n/a'}"))
    return out


def _pattern(entity, df, c):
    """
    Formato do identificador. Se a origem mudar 'P00001' para 'PLAYER-00001',
    nenhum outro check acusa — mas todo join do modelo para de casar. É o
    defeito mais barato de detectar e o mais caro de descobrir tarde.
    """
    out = []
    for col, rx in c.get("pattern", {}).items():
        if col not in df.columns:
            continue
        vals = df.loc[~_blank(df[col]), col].astype(str)
        bad = ~vals.str.fullmatch(rx, na=False)
        n = int(bad.sum())
        out.append(_r(entity, "id_pattern", col, ERROR, n == 0, n, len(df),
                      f"fora_do_padrao={n} regex={rx} exemplos={sorted(vals[bad].unique())[:5]}"))
    return out


def _no_spaces(entity, df, c):
    """
    Espaço nas pontas de uma chave é invisível na tela e quebra join em
    silêncio: 'P00001 ' != 'P00001'. Só faz sentido checar em coluna de chave.
    """
    out = []
    for col in c.get("no_spaces", []):
        if col not in df.columns:
            continue
        vals = df.loc[~_blank(df[col]), col].astype(str)
        n = int((vals != vals.str.strip()).sum())
        out.append(_r(entity, "no_surrounding_whitespace", col, ERROR, n == 0, n, len(df),
                      f"com_espaco_nas_pontas={n}"))
    return out


def run(entity, df, contract, reference_date):
    """Schema primeiro: se ele falha, os demais avaliariam colunas que não existem."""
    schema = _schema(entity, df, contract)
    if not schema.passed:
        return [schema]
    return ([schema]
            + _rows(entity, df, contract)
            + _pk(entity, df, contract)
            + _business_key(entity, df, contract)
            + _not_blank(entity, df, contract)
            + _domains(entity, df, contract)
            + _dates(entity, df, contract, reference_date)
            + _numerics(entity, df, contract)
            + _ceiling(entity, df, contract)
            + _floor(entity, df, contract)
            + _pattern(entity, df, contract)
            + _no_spaces(entity, df, contract))


def reconciliation(entity, rows_in, rows_written, rows_quarantined):
    """
    O invariante que torna a limpeza auditável:

        linhas_lidas == linhas_gravadas + linhas_quarentenadas

    Enquanto ele valer, nenhuma linha some em silêncio — ou seguiu, ou está na
    quarentena com o motivo. É o que separa "limpei os dados" de "sei
    exatamente o que descartei e por quê". Se quebrar, é bug no código de
    limpeza, não no dado: ERROR.
    """
    diff = rows_in - (rows_written + rows_quarantined)
    return _r(entity, "reconciliation", None, ERROR, diff == 0, abs(diff), rows_in,
              f"lidas={rows_in} gravadas={rows_written} "
              f"quarentena={rows_quarantined} diferenca={diff}")


def run_foreign_keys(frames, contracts):
    """
    Roda depois, com todas as entidades em memória. Órfão significa que os
    arquivos vieram de recortes temporais diferentes — o defeito mais silencioso
    de um pipeline multi-arquivo, porque o join perde linhas sem erro nenhum.
    """
    out = []
    for entity, contract in contracts.items():
        df = frames.get(entity)
        if df is None:
            continue
        for col, target in contract.get("fks", {}).items():
            parent_entity, parent_col = target.split(".")
            parent = frames.get(parent_entity)
            if parent is None or col not in df.columns or parent_col not in parent.columns:
                out.append(_r(entity, "referential_integrity", col, WARN, True, 0, len(df),
                              f"pulado: '{parent_entity}' nao carregada nesta execucao"))
                continue
            keys = set(parent[parent_col].dropna().astype(str))
            child = df.loc[~_blank(df[col]), col].astype(str)
            orphan = ~child.isin(keys)
            n = int(orphan.sum())
            out.append(_r(entity, "referential_integrity", col, ERROR, n == 0, n, len(df),
                          f"orfaos={n} exemplos={sorted(child[orphan].unique())[:10]} "
                          f"referencia={target}"))
    return out


# --------------------------------------------------------------------------
# Agregação para métricas
# --------------------------------------------------------------------------

CHECK_TO_METRIC = {
    "primary_key_unique": "DuplicateKeys",
    "primary_key_conflict": "DuplicateKeys",
    "duplicate_event": "DuplicateKeys",
    "reconciliation": "ReconciliationDiff",
    "not_blank": "BlankValues",
    "accepted_values": "DomainViolations",
    "timestamp_parseable": "UnparseableTimestamps",
    "not_future_dated": "FutureDatedRecords",
    "numeric_parseable": "UnparseableNumerics",
    "numeric_range": "OutOfRangeNumerics",
    "value_ceiling": "OutOfRangeNumerics",
    "value_floor": "OutOfRangeNumerics",
    "id_pattern": "FormatViolations",
    "no_surrounding_whitespace": "FormatViolations",
    "referential_integrity": "OrphanRecords",
}


def metrics_from(results):
    """Converte resultados em métricas. O logger só emite; quem conta é aqui."""
    m = {name: 0 for name in CHECK_TO_METRIC.values()}
    m["ChecksTotal"] = len(results)
    m["ChecksFailedError"] = sum(1 for r in results if not r.passed and r.severity == ERROR)
    m["ChecksFailedWarn"] = sum(1 for r in results if not r.passed and r.severity == WARN)
    for r in results:
        key = CHECK_TO_METRIC.get(r.check)
        if key:
            m[key] += r.failed_records
    return m


def errors(results):
    return [r for r in results if not r.passed and r.severity == ERROR]


def warnings(results):
    return [r for r in results if not r.passed and r.severity == WARN]
