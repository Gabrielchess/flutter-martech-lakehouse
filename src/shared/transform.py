"""
Motor de tipagem e conversão monetária.

Mesma divisão do data_quality: aqui fica a MECÂNICA genérica, e as regras de
cada entidade ficam no handler da camada. O motor não sabe o que é um
`deposit_id`; recebe uma especificação e executa.

SOBRE DINHEIRO E PONTO FLUTUANTE
--------------------------------
Os valores são convertidos em float64 e arredondados explicitamente a 2 casas
no fim. Não é o ideal teórico — decimal128 seria — mas as magnitudes aqui
(máximo ~2.000, taxas ~5,4) estão muitas ordens de grandeza longe do limite de
precisão do float64 (~15 dígitos significativos), e o arredondamento explícito
elimina o acúmulo. Em um sistema com valores grandes ou muitas operações
encadeadas, isto deveria ser decimal.
"""

import pandas as pd

BRL = "BRL"


# --------------------------------------------------------------------------
# Tipagem
# --------------------------------------------------------------------------

def cast(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    """
    Converte texto cru em tipos reais.

    spec aceita:
      "timestamps":  [col, ...]     ISO-8601 -> datetime UTC
      "dates":       [col, ...]     YYYY-MM-DD -> date
      "numbers":     [col, ...]     -> float64
      "booleans":    [col, ...]     'true'/'false' -> bool
      "fill":        {col: valor}   vazio -> valor explícito

    Erro de parse vira NaT/NaN, nunca exceção: quem decide se isso reprova é o
    contrato de qualidade, não o cast. Assim uma linha ruim não derruba o lote
    inteiro antes de ser medida.
    """
    d = df.copy()

    for col in spec.get("timestamps", []):
        if col in d:
            d[col] = pd.to_datetime(d[col], errors="coerce", utc=True, format="mixed")

    for col in spec.get("dates", []):
        if col in d:
            d[col] = pd.to_datetime(d[col], errors="coerce", format="mixed").dt.date

    for col in spec.get("numbers", []):
        if col in d:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    for col in spec.get("booleans", []):
        if col in d:
            d[col] = d[col].astype(str).str.strip().str.lower().map(
                {"true": True, "1": True, "yes": True,
                 "false": False, "0": False, "no": False})

    # Vazio vira valor EXPLÍCITO, nunca sumindo da base. Os 22 jogadores sem
    # acquisition_channel são 8,8% da carteira: descartá-los tiraria quase um
    # décimo do LTV por canal sem ninguém perceber.
    for col, value in spec.get("fill", {}).items():
        if col in d:
            blank = d[col].isna() | (d[col].astype(str).str.strip() == "")
            d.loc[blank, col] = value

    return d


# --------------------------------------------------------------------------
# Conversão monetária
# --------------------------------------------------------------------------

def load_fx(fx: pd.DataFrame) -> pd.DataFrame:
    """Recorta a tabela densa nas colunas do join, só o que converte para BRL."""
    cols = ["rate_date", "from_currency", "rate", "source_rate_date"]
    return fx.loc[fx.to_currency == BRL, cols].drop_duplicates(
        subset=["rate_date", "from_currency"])


def to_brl(df: pd.DataFrame, fx: pd.DataFrame, amount_cols, currency_col,
           date_col) -> pd.DataFrame:
    """
    Converte para BRL usando a taxa DA DATA DA TRANSAÇÃO.

    O join é igualdade simples — sem window, sem correlacionada — porque a
    tabela de câmbio é densa: existe linha para todo dia corrido, inclusive
    fim de semana e feriado. Com a resposta crua da API (só dia útil), ~32%
    das transações em moeda estrangeira não casariam e sumiriam sem erro.

    Grava a taxa usada e a data de origem dela (fx_rate_date), que difere de
    rate_date em fim de semana e feriado. Sem esse rastro, "por que este
    depósito virou R$ X" é irrespondível meses depois.
    """
    d = df.copy()
    d["_join_date"] = pd.to_datetime(d[date_col], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

    rates = load_fx(fx)
    d = d.merge(rates, how="left",
                left_on=["_join_date", currency_col],
                right_on=["rate_date", "from_currency"])

    d = d.rename(columns={"rate": "fx_rate", "source_rate_date": "fx_rate_date"})

    for col in amount_cols:
        d[f"{col}_brl"] = (d[col] * d["fx_rate"]).round(2)

    return d.drop(columns=["_join_date", "rate_date", "from_currency"], errors="ignore")


def missing_rate_count(df: pd.DataFrame) -> int:
    """Linhas que não acharam taxa. Qualquer valor > 0 é receita sumindo em silêncio."""
    return int(df["fx_rate"].isna().sum()) if "fx_rate" in df else 0
