import logging

import requests


def log_response(response: requests.Response):
    request = response.request

    request_string = f"{request.method} {request.url}"
    request_header_string = "\n".join([f"{k}: {v}" for k, v in request.headers.items()])
    logging.error(f"request:\n {request_string}\n {request_header_string}\n {request.body}\n")

    response_string = f"{response.status_code} {requests.codes.get(response.status_code)}"
    response_header_string = "\n".join([f"{k}: {v}" for k, v in response.headers.items()])
    logging.error(f"response:\n {response_string}\n {response_header_string}\n {response.content}")
