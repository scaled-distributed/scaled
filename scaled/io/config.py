import os

POLLING_TIME_MILLISECONDS = 20

# scheduler
DEFAULT_WORKER_TIMEOUT_SECONDS = 30

# worker
DEFAULT_NUMBER_OF_WORKER = os.cpu_count() - 1
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 1
DEFAULT_FUNCTION_RETENTION_SECONDS = 3600
DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS = 30
DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES = 1024 * 1024 * 1024
DEFAULT_PER_WORKER_QUEUE_SIZE = 1000