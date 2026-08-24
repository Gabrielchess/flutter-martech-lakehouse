"""
Parser da taxonomia de nome de campanha.

Padrão: {geo}_{channel}_{objective}_{product}_{audience}_{period}_{offer}

NÃO É PARSE POSICIONAL. Os 23 termos dos 6 vocabulários controlados são todos
únicos entre si — nenhuma palavra aparece em dois segmentos. Logo a posição não
carrega informação que o token já não carregue: se achou 'casino', só pode ser
product. Casar por pertencimento resolve nome com ordem trocada (C006, C012),
que a leitura posicional não resolveria.

E posicional não é só frágil, é perigoso: em 'BR_email_reactivation_2024Q1_
bonus50' (5 segmentos) a posição 4 seria '2024Q1', virando product='2024Q1'.
Valor errado é pior que ausente, porque não dispara nada.
"""

import difflib
import re

ORDER = ["geo", "channel", "objective", "product", "audience", "period", "offer"]
UNKNOWN = "unknown"
FUZZY_CUTOFF = 0.82


def _tokens(name):
    """
    Minúsculas, quebra em _ - ou espaço, descarta vazios.

    O teste é isinstance, não `name or ""`: campo vazio no CSV vira NaN, e
    float('nan') é TRUTHY em Python — passaria direto e estouraria no .lower().
    É o caso da C007, de nome em branco.
    """
    if not isinstance(name, str):
        return []
    return [t for t in re.split(r"[_\-\s]+", name.lower()) if t]


def parse(name, vocab):
    """
    name  -> str cru
    vocab -> {segmento: [termos]}, sem 'period' (é regex, não lista)

    Devolve {segmento: termo}, com UNKNOWN onde não achou.
    """
    toks = _tokens(name)
    # Tokens vizinhos rejuntados só para match EXATO: recupera 'bonus_50' ->
    # 'bonus50'. Fora do fuzzy de propósito, senão viram fonte de falso positivo.
    exact_pool = set(toks) | {toks[i] + toks[i + 1] for i in range(len(toks) - 1)}

    out = {}
    for seg in ORDER:
        if seg == "period":
            m = re.search(r"(20\d\d)q(\d)", " ".join(toks))
            out[seg] = f"{m.group(1)}Q{m.group(2)}" if m else UNKNOWN
            continue

        terms = vocab[seg]
        lower = [t.lower() for t in terms]

        hit = next((t for t, l in zip(terms, lower) if l in exact_pool), None)
        if hit is None:
            # Fuzzy só no que não casou exato, e só se UM termo do vocabulário
            # for candidato. Empate não vira palpite — vira unknown.
            found = {lower.index(m[0]) for t in toks
                     for m in [difflib.get_close_matches(t, lower, n=2, cutoff=FUZZY_CUTOFF)]
                     if len(m) == 1}
            hit = terms[found.pop()] if len(found) == 1 else None

        out[seg] = hit or UNKNOWN

    return out


def standard_name(parsed):
    """Reescreve no padrão oficial. É o nome a devolver ao time de CRM."""
    return "_".join(parsed[seg] for seg in ORDER)
