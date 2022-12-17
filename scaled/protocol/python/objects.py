import enum


class MessageType(enum.Enum):
    Task = b"TK"
    TaskCancel = b"TC"
    TaskEcho = b"TE"
    TaskResult = b"TR"
    Heartbeat = b"IF"


class TaskStatus(enum.Enum):
    Success = b"S"
    Failed = b"F"
    Canceled = b"C"
