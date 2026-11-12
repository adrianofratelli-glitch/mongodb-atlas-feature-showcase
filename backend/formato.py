"""Formatação de valores para a UI, que é em pt-BR."""


def brl(valor, casas: int = 2) -> str:
    """12345.6 → 'R$ 12.345,60'. Sem depender de locale do sistema (o container não tem pt_BR)."""
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return "R$ —"
    texto = f"{abs(numero):,.{casas}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"{'-' if numero < 0 else ''}R$ {texto}"
