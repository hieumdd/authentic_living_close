import os
import json
import asyncio
from datetime import datetime
from abc import ABCMeta, abstractmethod

import requests
import aiohttp
from google.cloud import bigquery

LIMIT = 100
BASE_URL = "https://api.close.com/api/v1"
AUTH = (os.getenv("AUTH_KEY"), "")

BQ_CLIENT = bigquery.Client()
DATASET = "Close"

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"


def get_time_range(model, _start, _end):
    if _start and _end:
        start, end = [datetime.strptime(i, DATE_FORMAT) for i in [_start, _end]]
    else:
        query = f"""
        SELECT MAX({model.keys['incre_key']}) AS max_incre
        FROM {DATASET}.{model.table}"""
        result = BQ_CLIENT.query(query).result()
        max_incre = [dict(i.items()) for i in result][0]["max_incre"]
        start = max_incre
        end = NOW
    return start, end


class Getter(metaclass=ABCMeta):
    def __init__(self, model):
        self.endpoint = model.endpoint
        self.url = f"{BASE_URL}/{self.endpoint}/"

    @abstractmethod
    def get(self):
        pass


class SimpleGetter(Getter):
    def get(self):
        rows = []
        params = {
            "_limit": LIMIT,
            "_skip": 0,
        }
        with requests.Session() as session:
            while True:
                with session.get(self.url, params=params, auth=AUTH) as r:
                    res = r.json()
                rows.extend(res["data"])
                has_more = res.get("has_more")
                if has_more:
                    params["_skip"] += LIMIT
                else:
                    break
        return rows


class AsyncGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.start, self.end = model.start, model.end
        self.params = model.params

    def get(self):
        return asyncio.run(self._get_async())

    def _get_params(self, skip=0):
        return {
            **self.params,
            "_limit": LIMIT,
            "_skip": skip,
        }

    async def _get_async(self):
        async with aiohttp.ClientSession() as session:
            count = await self._get_count(session)
            pages = [i for i in range(0, count, LIMIT)]
            tasks = [asyncio.create_task(self._get_one(session, i)) for i in pages]
            rows = await asyncio.gather(*tasks)
        rows = [item for sublist in rows for item in sublist]
        return rows

    async def _get_count(self, session):
        params = self._get_params()
        async with session.get(
            self.url,
            params=params,
            auth=aiohttp.BasicAuth(*AUTH),
        ) as r:
            res = await r.json()
        return res["total_results"]

    async def _get_one(self, session, i):
        params = self._get_params(i)
        async with session.get(
            self.url,
            params=params,
            auth=aiohttp.BasicAuth(*AUTH),
        ) as r:
            res = await r.json()
        return res["data"]


class Close(metaclass=ABCMeta):
    @staticmethod
    def factory(table, start, end):
        if table == "Leads":
            return Leads(start, end)
        elif table == "Opportunities":
            return Opportunities(start, end)
        elif table == "CustomActivities":
            return CustomActivities()
        elif table == "Users":
            return Users()
        elif table == "CustomFields":
            return CustomFields()
        else:
            raise NotImplementedError(table)

    @property
    @abstractmethod
    def endpoint(self):
        pass

    @abstractmethod
    def transform(self, rows):
        return rows

    def _load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT (row_num) FROM
        (
            SELECT *,
            ROW_NUMBER() OVER
            (PARTITION BY {','.join(self.keys['p_key'])} ORDER BY {self.keys['incre_key']} DESC)
            AS row_num
            FROM {DATASET}._stage_{self.table}
        ) WHERE row_num = 1"""
        BQ_CLIENT.query(query).result()

    def run(self):
        rows = self.getter.get()
        response = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if getattr(self.getter, "start", None) and getattr(self.getter, "end", None):
            response["start"] = self.getter.start
            response["end"] = self.getter.end
        if len(rows):
            rows = self.transform(rows)
            loads = self._load(rows)
            self._update()
            response["output_rows"] = loads.output_rows
        return response


class Leads(Close):
    endpoint = "lead"
    table = "Leads"
    keys = {
        "p_key": ["id"],
        "incre_key": "date_updated",
    }
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "display_name", "type": "STRING"},
        {"name": "updated_by", "type": "STRING"},
        {"name": "status_id", "type": "STRING"},
        {"name": "created_by", "type": "STRING"},
        {"name": "custom_lead_owner", "type": "STRING"},
        {"name": "organization_id", "type": "STRING"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {
            "name": "contacts",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "phones",
                    "type": "record",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "phone_formatted", "type": "STRING"},
                        {"name": "phone", "type": "STRING"},
                        {"name": "type", "type": "STRING"},
                        {"name": "country", "type": "STRING"},
                    ],
                },
                {"name": "name", "type": "STRING"},
                {"name": "updated_by", "type": "STRING"},
                {
                    "name": "emails",
                    "type": "record",
                    "mode": "repeated",
                    "fields": [
                        {"name": "type", "type": "STRING"},
                        {"name": "email", "type": "STRING"},
                    ],
                },
                {"name": "date_updated", "type": "TIMESTAMP"},
                {"name": "display_name", "type": "STRING"},
                {"name": "date_created", "type": "TIMESTAMP"},
                {"name": "lead_id", "type": "STRING"},
                {"name": "created_by", "type": "STRING"},
                {"name": "title", "type": "STRING"},
                {"name": "id", "type": "STRING"},
            ],
        },
    ]

    @property
    def params(self):
        start, end = [i.strftime(DATE_FORMAT) for i in [self.start, self.end]]
        return {
            "query": f"date_updated > {start} date_updated < {end}",
        }

    def __init__(self, start, end):
        self.start, self.end = self.get_time_range(self, start, end)
        self.getter = AsyncGetter(self)

    def transform(self, rows):
        rows = [
            {
                "id": row["id"],
                "date_updated": row["date_updated"],
                "display_name": row.get("display_name"),
                "updated_by": row.get("updated_by"),
                "status_id": row.get("status_id"),
                "created_by": row.get("created_by"),
                "custom_lead_owner": row.get("custom_lead_owner"),
                "organization_id": row.get("organization_id"),
                "date_created": row.get("date_created"),
                #
                "contacts": [
                    {
                        "phones": [
                            {
                                "phone_formatted": phone.get("phone_formatted"),
                                "phone": phone.get("phone"),
                                "type": phone.get("type"),
                                "country": phone.get("country"),
                            }
                            for phone in contact.get("phones")
                        ]
                        if contact.get("phones")
                        else [],
                        "name": contact.get("name"),
                        "updated_by": contact.get("updated_by"),
                        "emails": [
                            {
                                "type": email.get("type"),
                                "email": email.get("email"),
                            }
                            for email in contact.get("emails")
                        ]
                        if contact.get("emails")
                        else [],
                        "date_updated": contact.get("date_updated"),
                        "display_name": contact.get("display_name"),
                        "date_created": contact.get("date_created"),
                        "lead_id": contact.get("lead_id"),
                        "created_by": contact.get("created_by"),
                        "title": contact.get("title"),
                        "id": contact.get("id"),
                    }
                    for contact in row.get("contacts")
                ]
                if row.get("contacts")
                else [],
            }
            for row in rows
        ]
        return rows


class CustomActivities(Close):
    endpoint = "activity/custom"
    table = "CustomActivities"
    keys = {
        "p_key": ["lead_id"],
        "incre_key": "date_created",
    }
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "lead_id", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "custom_activity_type_id", "type": "STRING"},
        {"name": "_type", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {
            "name": "custom_fields",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "key", "type": "STRING"},
                {"name": "value", "type": "STRING"},
            ],
        },
    ]

    def __init__(self):
        self.getter = SimpleGetter(self)

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "date_created": row["date_created"],
                "date_updated": row["date_updated"],
                "lead_id": row.get("lead_id"),
                "user_id": row.get("user_id"),
                "custom_activity_type_id": row.get("custom_activity_type_id"),
                "_type": row.get("_type"),
                "status": row.get("status"),
                "custom_fields": [
                    {
                        "key": key,
                        "value": json.dumps(value),
                    }
                    for key, value in row.items()
                    if "custom.cf" in key
                ],
            }
            for row in rows
        ]


class Users(Close):
    endpoint = "user"
    table = "Users"
    keys = {
        "p_key": ["id"],
        "incre_key": "date_updated",
    }
    schema = [
        {"name": "email", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "last_used_timezone", "type": "STRING"},
        {"name": "email_verified_at", "type": "TIMESTAMP"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {"name": "image", "type": "STRING"},
    ]

    def __init__(self):
        super().__init__()
        self.getter = SimpleGetter(self)

    def transform(self, rows):
        return [
            {
                "email": row.get("email"),
                "id": row.get("id"),
                "first_name": row.get("first_name"),
                "last_name": row.get("last_name"),
                "date_updated": row.get("date_updated"),
                "last_used_timezone": row.get("last_used_timezone"),
                "email_verified_at": row.get("email_verified_at"),
                "date_created": row.get("date_created"),
                "image": row.get("image"),
            }
            for row in rows
        ]


class Opportunities(Close):
    endpoint = "opportunity"
    table = "Opportunities"
    keys = {
        "p_key": ["id"],
        "incre_key": "date_updated",
    }
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "updated_by", "type": "STRING"},
        {"name": "created_by", "type": "STRING"},
        {"name": "organization_id", "type": "STRING"},
        {"name": "contact_name", "type": "STRING"},
        {"name": "lead_id", "type": "STRING"},
        {"name": "expected_value", "type": "NUMERIC"},
        {"name": "status_display_name", "type": "STRING"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {"name": "annualized_value", "type": "NUMERIC"},
        {"name": "status_id", "type": "STRING"},
        {"name": "status_type", "type": "STRING"},
        {"name": "value", "type": "NUMERIC"},
        {"name": "value_currency", "type": "STRING"},
        {"name": "note", "type": "STRING"},
        {"name": "value_period", "type": "STRING"},
        {"name": "status_label", "type": "STRING"},
        {"name": "lead_name", "type": "STRING"},
        {"name": "created_by_name", "type": "STRING"},
        {"name": "updated_by_name", "type": "STRING"},
        {"name": "user_name", "type": "STRING"},
        {"name": "annualized_expected_value", "type": "NUMERIC"},
        {"name": "value_formatted", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
        {"name": "date_won", "type": "DATE"},
        {"name": "date_lost", "type": "DATE"},
        {
            "name": "custom_fields",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "key", "type": "STRING"},
                {"name": "value", "type": "STRING"},
            ],
        },
    ]

    @property
    def params(self):
        return {
            "date_updated__gte": self.start.strftime(DATE_FORMAT),
            "date_updated__lte": self.end.strftime(DATE_FORMAT),
        }

    def __init__(self, start, end):
        self.start, self.end = get_time_range(self, start, end)
        self.getter = AsyncGetter(self)

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "date_updated": row["date_updated"],
                "updated_by": row.get("updated_by"),
                "created_by": row.get("created_by"),
                "organization_id": row.get("organization_id"),
                "contact_name": row.get("contact_name"),
                "lead_id": row.get("lead_id"),
                "expected_value": row.get("expected_value"),
                "status_display_name": row.get("status_display_name"),
                "date_created": row.get("date_created"),
                "annualized_value": row.get("annualized_value"),
                "status_id": row.get("status_id"),
                "status_type": row.get("status_type"),
                "value": row.get("value"),
                "value_currency": row.get("value_currency"),
                "note": row.get("note"),
                "value_period": row.get("value_period"),
                "status_label": row.get("status_label"),
                "lead_name": row.get("lead_name"),
                "created_by_name": row.get("created_by_name"),
                "updated_by_name": row.get("updated_by_name"),
                "user_name": row.get("user_name"),
                "annualized_expected_value": row.get("annualized_expected_value"),
                "annualized_value": row.get("annualized_value"),
                "value_formatted": row.get("value_formatted"),
                "user_id": row.get("user_id"),
                "contact_id": row.get("contact_id"),
                "date_won": row.get("date_won"),
                "date_lost": row.get("date_lost"),
                "custom_fields": [
                    {
                        "key": key,
                        "value": json.dumps(value),
                    }
                    for key, value in row.items()
                    if "custom.cf" in key
                ],
            }
            for row in rows
        ]


class CustomFields(Close):
    endpoint = "custom_activity"
    table = "CustomFields"
    keys = {
        "p_key": ["id"],
        "incre_key": "date_updated",
    }
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "organization_id", "type": "STRING"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "api_create_only", "type": "BOOLEAN"},
        {
            "name": "fields",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "required", "type": "BOOLEAN"},
                {"name": "type", "type": "STRING"},
                {"name": "converting_to_type", "type": "STRING"},
                {"name": "accepts_multiple_values", "type": "BOOLEAN"},
                {"name": "is_share", "type": "BOOLEAN"},
            ],
        },
    ]

    def __init__(self):
        self.getter = SimpleGetter(self)

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "name": row["name"],
                "organization_id": row["organization_id"],
                "date_created": row["date_created"],
                "date_updated": row["date_updated"],
                "api_create_only": row["api_create_only"],
                "fields": [
                    {
                        "id": field["id"],
                        "name": field["name"],
                        "required": field["required"],
                        "type": field["type"],
                        "converting_to_type": field["converting_to_type"],
                        "accepts_multiple_values": field["accepts_multiple_values"],
                        "is_share": field["is_shared"],
                    }
                    for field in row["fields"]
                ],
            }
            for row in rows
        ]
