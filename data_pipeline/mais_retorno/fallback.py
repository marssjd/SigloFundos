"""Manual fallback integration for Mais Retorno scraping."""
from __future__ import annotations

import logging
from pathlib import Path

LOGGER = logging.getLogger(__name__)

TERMS_URL = "https://www.maisretorno.com/termos"


def check_terms_of_use() -> None:
    LOGGER.warning(
        "O scraping do Mais Retorno deve respeitar os termos de uso: %s. "
        "Verifique a permissão antes de executar qualquer coleta manual.",
        TERMS_URL,
    )


def run_manual_scraper(destination: Path) -> None:
    """Placeholder for manual scraping steps.

    Esta função não executa nenhum scraping automaticamente. Ela apenas registra
    uma mensagem informando o operador sobre os passos necessários.
    """

    check_terms_of_use()
    LOGGER.info(
        "Para executar o fallback manual: exporte os dados do site Mais Retorno, "
        "salve-os como CSV e coloque o arquivo em %s antes de rodar a pipeline.",
        destination,
    )
