import zipfile
from pathlib import Path

import pytest

pytest.importorskip("pandas")

from data_pipeline.cvm import inf_diario, inf_mensal


def create_zip(tmp_path: Path, name: str, csv_name: str, content: str) -> Path:
    archive_path = tmp_path / name
    with zipfile.ZipFile(archive_path, "w") as zf:
        zf.writestr(csv_name, content)
    return archive_path


def test_inf_diario_load_csv(tmp_path):
    csv_content = "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST\n" "00.000.000/0000-00;2023-01-31;10;1;10;0;0;5\n"
    archive_path = create_zip(tmp_path, "inf_diario.zip", "inf_diario.csv", csv_content)
    df = inf_diario.load_csv_from_archive(archive_path)
    assert df.iloc[0]["CNPJ_FUNDO"] == "00.000.000/0000-00"


def test_inf_mensal_load_csv(tmp_path):
    carteira_csv = "CNPJ_FUNDO;DT_COMPTC;TP_APLIC;TP_ATIVO;EMISSOR;SETOR;COD_ISIN;VL_MERC_POS_FINAL;QT_POS_FINAL\n" "00.000.000/0000-00;2023-01-31;ACOES;BRASIL;EMPRESA;SETOR;ISIN;100;10\n"
    cotistas_csv = "CNPJ_FUNDO;DT_COMPTC;CLASSE_COTISTAS;QT_COTISTAS;VL_PATRIM_LIQ\n" "00.000.000/0000-00;2023-01-31;GERAL;5;100\n"
    archive_path = tmp_path / "inf_mensal.zip"
    with zipfile.ZipFile(archive_path, "w") as zf:
        zf.writestr("carteira.csv", carteira_csv)
        zf.writestr("cotistas.csv", cotistas_csv)

    carteira_df = inf_mensal.load_csv_from_archive(archive_path, pattern="carteira")
    cotistas_df = inf_mensal.load_csv_from_archive(archive_path, pattern="cotist")

    assert carteira_df.iloc[0]["TP_APLIC"] == "ACOES"
    assert cotistas_df.iloc[0]["QT_COTISTAS"] == "5"
