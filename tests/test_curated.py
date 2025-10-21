import pytest

pytest.importorskip("pandas")

import pandas as pd

from data_pipeline.run_pipeline import build_curated_tables


def test_curated_tables_grouping():
    fato = pd.DataFrame(
        {
            "cnpj": ["1", "1", "2"],
            "data_cotacao": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-02"]),
            "valor_cota": [1.0, 1.2, 1.5],
            "patrimonio_liquido": [100, 120, 200],
        }
    )
    dim_fundo = pd.DataFrame(
        {
            "cnpj": ["1", "2"],
            "categoria_cvm": ["A", "B"],
            "gestora": ["G1", "G2"],
            "grupo_looker": ["Grupo1", "Grupo2"],
        }
    )
    tables = {"fato_cota_diaria": fato, "dim_fundo": dim_fundo}
    curated = build_curated_tables(tables)
    assert "curated_cotas_por_categoria" in curated
    result = curated["curated_cotas_por_categoria"]
    assert set(result["categoria_cvm"]) == {"A", "B"}
