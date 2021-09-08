import pytest

from .utils import process

START = "2021-08-01"
END = "2021-08-31"

STATIC_TABLES = [
    "Users",
    "CustomFields",
]
INCREMENTAL_TALBES = [
    "Leads",
    "Opportunities",
    "CustomActivities",
]


@pytest.mark.parametrize(
    "table",
    STATIC_TABLES,
)
def test_static(table):
    data = {
        "table": table,
    }
    process(data)


@pytest.mark.parametrize(
    "table",
    INCREMENTAL_TALBES,
)
@pytest.mark.parametrize(
    "start,end",
    [
        (None, None),
        ("2021-08-01", "2021-08-31"),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_incremental(table, start, end):
    data = {
        "table": table,
        "start": start,
        "end": end,
    }
    process(data)
