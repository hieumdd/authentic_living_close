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

    def get(self):
        return asyncio.run(self._get_async())

    def _get_params(self, skip=0):
        start, end = [i.strftime(DATE_FORMAT) for i in [self.start, self.end]]
        return {
            "query": f"date_updated > {start} date_updated < {end}",
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
        if table == "Lead":
            return Lead(start, end)
        elif table == "CustomActivity":
            return CustomActivity()
        elif table == "User":
            return User()

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


class Lead(Close):
    endpoint = "lead"
    table = "Lead"
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
            "type": "record",
            "mode": "repeated",
            "fields": [
                {
                    "name": "phones",
                    "type": "record",
                    "mode": "repeated",
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

    def __init__(self, start, end):
        self.start, self.end = self._get_time_range(start, end)
        self.getter = AsyncGetter(self)

    def _get_time_range(self, _start, _end):
        if _start and _end:
            start, end = [datetime.strptime(i, DATE_FORMAT) for i in [_start, _end]]
        else:
            query = f"""
            SELECT MAX({self.keys['incre_key']}) AS max_incre
            FROM {DATASET}.{self.table}"""
            result = BQ_CLIENT.query(query).result()
            max_incre = [dict(i.items()) for i in result][0]["max_incre"]
            start = max_incre
            end = NOW
        return start, end

    def transform(self, rows):
        rows
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


class CustomActivity(Close):
    endpoint = "activity/custom"
    table = "new_close_activities"
    keys = {
        "p_key": ["lead_id"],
        "incre_key": "date_updated",
    }
    with open("configs/CustomActivity.json") as f:
        schema = json.load(f)["schema"]

    def __init__(self):
        super().__init__()
        self.getter = SimpleGetter(self)

    def transform(self, rows):
        rows
        return [
            {
                "lead_id": json.dumps(row.get("lead_id")),
                "date_updated": row.get("date_updated"),
                "cash": json.dumps(
                    row.get("custom.acf_uoVlV2xEOMqk95VN47QzUtIv0gjJ45q5yupojyxoncr")
                ),
                "date_closed": json.dumps(
                    row.get("custom.acf_gTTx1MCvzyDGiCge4vIUYQIF8dkSYPqpmqbBsnbspic")
                ),
                "number_of_payments": json.dumps(
                    row.get("custom.acf_BTO01d4tPWzPoPI2s5VFvum7RQ9HzlhOl43WEIbo2IN")
                ),
                "revenue": json.dumps(
                    row.get("custom.acf_GQutYFmaWGyLQOuCZ6UnYEncMwnpp1vbGjiA0LX2sLj")
                ),
                "setter": json.dumps(
                    row.get("custom.acf_v4B0U6VlVEJxMRBkrOFr2if5NQzNS3gEqyyckfUb9uX")
                ),
                "sold_by": json.dumps(
                    row.get("custom.acf_A6lByV6qUWTnHY72u03XpOD7cV6eiY7I0mq3V4SeQan")
                ),
            }
            for row in rows
        ]


class User(Close):
    endpoint = "user"
    table = "new_close_users"
    keys = {
        "p_key": ["id"],
        "incre_key": "date_updated",
    }
    with open("configs/User.json") as f:
        schema = json.load(f)["schema"]

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
