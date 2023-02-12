# Scaled
This project is aiming the target that provides simple and efficient and reliable way for distributing computing 
framework, centralized scheduler and stable protocol when client and worker talking to scheduler

# Introduction
The goal for this project should be as simple as possible
- It built on top of zmq
- it has ready python version of Client, Scheduler, Worker
- I will provide golang or Rust version of Scheduler, the goal for the Scheduler should be completely computer language 
  agnostic, which means they follow the same protocol
- Scheduler might support function based computing tree in the future

# Installation
`pip install scaled`

if you want to use uvloop, please do: `pip install uvloop`, default we are using python builtin uvloop


# How to use it

## Start local scheduler and cluster at the same time in the code

```python
import random

from scaled.client import Client
from scaled.cluster.combo import SchedulerClusterCombo


def calculate(sec: int):
    return sec * 1


def main():
    address = "tcp://127.0.0.1:2345"

    cluster = SchedulerClusterCombo(address=address, n_workers=10, event_loop="uvloop")
    client = Client(address=address)

    tasks = [random.randint(0, 100) for _ in range(100000)]
    futures = [client.submit(calculate, i) for i in tasks]

    results = [future.result() for future in futures]

    assert results == tasks

    client.disconnect()
    cluster.shutdown()


if __name__ == "__main__":
    main()
```

## Start scheduler and cluster independently

use `scaled_scheduler` to start scheduler, for example:
```bash
scaled_scheduler tcp://0.0.0.0:8516
```

use `scaled_cluster` to start 10 workers:
```bash
scaled_worker -n 10 tcp://127.0.0.1:8516
```

for detail options of above 2 program, please use argument `-h` to check out all available options

Then you can write simply write client code as:

```python
from scaled.client import Client


def foobar(foo: int):
    return foo


client = Client(address="tcp://127.0.0.1:2345")
future = client.submit(foobar, 1)

print(future.result())
```