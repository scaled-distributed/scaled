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
$ scaled_scheduler tcp://0.0.0.0:8516
[INFO]2023-03-19 12:16:10-0400: logging to ('/dev/stdout',)
[INFO]2023-03-19 12:16:10-0400: use event loop: 2
[INFO]2023-03-19 12:16:10-0400: Scheduler: monitor address is ipc:///tmp/0.0.0.0_8516_monitor
[INFO]2023-03-19 12:16:10-0400: AsyncBinder: started
[INFO]2023-03-19 12:16:10-0400: VanillaTaskManager: started
[INFO]2023-03-19 12:16:10-0400: VanillaFunctionManager: started
[INFO]2023-03-19 12:16:10-0400: VanillaWorkerManager: started
[INFO]2023-03-19 12:16:10-0400: StatusReporter: started
```

use `scaled_cluster` to start 10 workers:
```bash
$ scaled_worker -n 10 tcp://127.0.0.1:8516
[INFO]2023-03-19 12:19:19-0400: logging to ('/dev/stdout',)
[INFO]2023-03-19 12:19:19-0400: ClusterProcess: starting 23 workers, heartbeat_interval_seconds=2, function_retention_seconds=3600
[INFO]2023-03-19 12:19:19-0400: Worker[0] started
[INFO]2023-03-19 12:19:19-0400: Worker[1] started
[INFO]2023-03-19 12:19:19-0400: Worker[2] started
[INFO]2023-03-19 12:19:19-0400: Worker[3] started
[INFO]2023-03-19 12:19:19-0400: Worker[4] started
[INFO]2023-03-19 12:19:19-0400: Worker[5] started
[INFO]2023-03-19 12:19:19-0400: Worker[6] started
[INFO]2023-03-19 12:19:19-0400: Worker[7] started
[INFO]2023-03-19 12:19:19-0400: Worker[8] started
[INFO]2023-03-19 12:19:19-0400: Worker[9] started
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

Scaled also supports submit graph task, for example:

```python
from scaled.client import Client


def inc(i):
    return i + 1

def add(a, b):
    return a + b

def minus(a, b):
    return a - b

graph = {
    "a": 2,
    "b": 2, 
    "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
    "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
    "e": (minus, "d", "c")  # e = d - c = 4 - 3 = 1
}

client = Client(address="tcp://127.0.0.1:2345")
futures = client.submit_graph(graph, keys=["e"])

print(futures[0].result())
```

# Scaled Top
You can use `scaled_top` to connect to scheduler monitor address to get some insides of the scaled_top
```bash
$ scaled_top ipc:///tmp/0.0.0.0_8516_monitor
```

Which will something similar to top command, but it's for getting status of the scaled system:
```bash
scheduler          | task_manager         |   scheduler_sent         | scheduler_received
      cpu     0.0% |   unassigned       0 | FunctionResponse      24 |          Heartbeat 183,109
      rss 37.1 MiB |      running       0 |         TaskEcho 200,000 |    FunctionRequest      24
                   |      success 200,000 |             Task 200,000 |               Task 200,000
                   |       failed       0 |       TaskResult 200,000 |         TaskResult 200,000
                   |     canceled       0 |   BalanceRequest       4 |    BalanceResponse       4
--------------------------------------------------------------------------------------------------
Shortcuts: worker[n] cpu[c] rss[m] free[f] working[w] queued[q]

Total 10 worker(s)
                 worker agt_cpu agt_rss [cpu]   rss free sent queued | function_id_to_tasks
W|Linux|15940|3c9409c0+    0.0%   32.7m  0.0% 28.4m 1000    0      0 |
W|Linux|15946|d6450641+    0.0%   30.7m  0.0% 28.2m 1000    0      0 |
W|Linux|15942|3ed56e89+    0.0%   34.8m  0.0% 30.4m 1000    0      0 |
W|Linux|15944|6e7d5b99+    0.0%   30.8m  0.0% 28.2m 1000    0      0 |
W|Linux|15945|33106447+    0.0%   31.1m  0.0% 28.1m 1000    0      0 |
W|Linux|15937|b031ce9a+    0.0%   31.0m  0.0% 30.3m 1000    0      0 |
W|Linux|15941|c4dcc2f3+    0.0%   30.5m  0.0% 28.2m 1000    0      0 |
W|Linux|15939|e1ab4340+    0.0%   31.0m  0.0% 28.1m 1000    0      0 |
W|Linux|15938|ed582770+    0.0%   31.1m  0.0% 28.1m 1000    0      0 |
W|Linux|15943|a7fe8b5e+    0.0%   30.7m  0.0% 28.3m 1000    0      0 |
```

- scheduler section is showing how much resources scheduler used
- task_manager section shows count for each task status
- scheduler_sent section shows count for each type of messages scheduler sent
- scheduler_received section shows count for each type of messages scheduler received
- function_id_to_tasks section shows task count for each function used
- worker section shows worker details, you can use shortcuts to sort by columns, the char * on column header show which 
  column is sorted right now
  - agt_cpu/agt_rss means cpu/memory usage of worker agent
  - cpu/rss means cpu/memory usage of worker
  - free means number of free task slots for this worker
  - sent means how many tasks scheduler sent to the worker
  - queued means how many tasks worker received and queued