import argparse
import json

from scaled.client import Client


def get_args():
    parser = argparse.ArgumentParser(
        "poke scheduler for monitoring information", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("address", help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    client = Client(address=args.address)
    print(json.dumps(client.scheduler_status(), indent=4))
    client.disconnect()