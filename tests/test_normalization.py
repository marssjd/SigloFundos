import pytest

pytest.importorskip("pandas")

from data_pipeline.common import normalization


def test_normalize_cnpj_strips_characters():
    assert normalization.normalize_cnpj("12.345.678/0001-90") == "12345678000190"


def test_parse_date_multiple_formats():
    parsed = normalization.parse_date("2023-05-01", ["%d/%m/%Y", "%Y-%m-%d"])
    assert parsed.year == 2023 and parsed.month == 5 and parsed.day == 1
