import base64
import json

from models import Close


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    job = Close.factory(
        data["table"],
        data.get("start"),
        data.get("end"),
    )
    results = job.run()
    response = {
        "pipelines": "Wufoo",
        "results": results,
    }
    print(response)
    return response
