import json
import base64
from unittest.mock import Mock

from main import main


def encode_data(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {
        "message": {
            "data": data_encoded,
        },
    }


def assertion(res):
    res = res['results']
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]


def process(data):
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def process_broadcast(data):
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["results"]["message_sent"] > 0
