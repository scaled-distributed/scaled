import os

# ==============
# SYSTEM OPTIONS
POLLING_TIME_MILLISECONDS = 50

# clean up routine interval seconds
CLEANUP_INTERVAL_SECONDS = 1

# ==========================
# SCHEDULER SPECIFIC OPTIONS

# number of threads for zmq socket to handle
DEFAULT_IO_THREADS = 1

# if all workers are full and busy working, this option determine how many additional tasks scheduler can receive and
# queued, if additional number of tasks received exceeded this number, scheduler will reject tasks
DEFAULT_MAX_NUMBER_OF_TASKS_WAITING = -1

# if didn't receive heartbeat for following seconds, then scheduler will treat worker as dead and reschedule unfinished
# tasks for this worker
DEFAULT_WORKER_TIMEOUT_SECONDS = 60

# status report interval, used by scaled top or customized monitor
STATUS_REPORT_INTERVAL_SECONDS = 1

# number of seconds for load balance, if value is 0 means disable load balance
DEFAULT_LOAD_BALANCE_SECONDS = 1

# when load balance advice happened repeatedly and always be the same, we issue load balance request when exact repeated
# times happened
DEFAULT_LOAD_BALANCE_TRIGGER_TIMES = 2

# number of tasks can be queued to each worker on scheduler side
DEFAULT_PER_WORKER_QUEUE_SIZE = 1000

# number of seconds the function cache will be kept in scheduler's memory, please note if this retention time is up,
# it will issue signal to delete outdated function cache in all connected worker, which means if below worker option
# DEFAULT_WORKER_FUNCTION_RETENTION_SECONDS is set higher than this option, it might not take effect
DEFAULT_FUNCTION_RETENTION_SECONDS = 20

# =======================
# WORKER SPECIFIC OPTIONS

# number of workers, echo worker use 1 process
DEFAULT_NUMBER_OF_WORKER = int(os.cpu_count()) - 1  # type: ignore

# number of seconds that worker agent send heartbeat to scheduler
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 2

# number of seconds worker(s) will quit if didn't hear from scheduler
DEFAULT_DEATH_TIMEOUT_SECONDS = 10

# number of seconds worker doing garbage collection
DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS = 30

# number of bytes threshold for worker process that trigger deep garbage collection
DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES = 1024 * 1024 * 1024  # 1GiB

# number of seconds the function cache will be kept in worker's memory, but please note worker can still delete the
# function cache when receive the signal from scheduler
DEFAULT_WORKER_FUNCTION_RETENTION_SECONDS = 20
