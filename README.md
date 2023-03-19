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

# Scaled Top
You can use `scaled_top` to connect to scheduler monitor address to get some insides of the scaled_top
```bash
$ scaled_top ipc:///tmp/0.0.0.0_8516_monitor
```

Which will something similar to top command, but it's for getting status of the scaled system:
```bash
scheduler              task_manager                 scheduler_sent             scheduler_received
      cpu     160.0%     unassigned         0     FunctionResponse       101            Heartbeat     48679
      rss   54.3 MiB        running         2             TaskEcho   1908688      FunctionRequest       101
                            success   1908686                 Task   1908688                 Task   1908688
                             failed         0           TaskResult   1908686           TaskResult   1908686
                           canceled         0       BalanceRequest        48      BalanceResponse        48
                                                DisconnectResponse       160    DisconnectRequest       160

Shortcuts: worker[n] cpu[c] rss[m] free[f] working[w] queued[q]
                   worker   *cpu       rss   free  working  queued        function_id_to_tasks
W|Windows|20678|4845f57b+  29.0%  30.6 MiB    999        1       0                   8e7b7fbe+  2
W|Windows|20679|3cc3bc1e+  25.0%  32.5 MiB  1,000        0       1
W|Windows|20680|95f1a794+  22.0%  32.2 MiB    999        1       1
W|Windows|20681|57554ceb+  18.0%  30.2 MiB  1,000        0       1
W|Windows|20682|4920c056+  15.0%  30.6 MiB  1,000        0       0
W|Windows|20683|695d7efb+  12.0%  32.4 MiB  1,000        0       1
W|Windows|20684|ef9f3c16+   8.0%  30.4 MiB  1,000        0       0
W|Windows|20685|e762963c+   3.0%  32.0 MiB  1,000        0       0
W|Windows|20686|fba5cc0d+   1.0%  32.4 MiB  1,000        0       0
W|Windows|20687|6b14b2d0+   0.0%  30.3 MiB  1,000        0       0
```

- scheduler section is showing how much resources scheduler used
- task_manager section shows count for each task status
- scheduler_sent section shows count for each type of messages scheduler sent
- scheduler_received section shows count for each type of messages scheduler received
- function_id_to_tasks section shows task count for each function used
- worker section shows worker details, you can use shortcuts to sort by columns, the char * on column header show which 
  column is sorted right now