import pytest

from .utils import process

START = "2021-08-01"
END = "2021-08-31"


def test_leads_auto():
    data = {
        "table": "Lead",
    }
    process(data)


def test_leads_manual():
    data = {
        "table": "Lead",
        "start": START,
        "end": END,
    }
    process(data)


@pytest.mark.parametrize("table", ["CustomActivity", "User"])
def test_auto(table):
    data = {
        "table": table,
    }
    process(data)
