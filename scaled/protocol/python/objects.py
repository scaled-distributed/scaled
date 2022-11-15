import enum


class MessageType(enum.Enum):
    Job = b"JM"
    JobEcho = b"JE"
    JobResult = b"JR"
    Task = b"TK"
    TaskEcho = b"TE"
    TaskResult = b"TR"
    Heartbeat = b"IF"


class JobStatus(enum.Enum):
    Success = b"S"
    Failed = b"F"
    Canceled = b"C"
