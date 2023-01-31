import enum


class MessageType(enum.Enum):
    Task = b"TK"
    TaskEcho = b"TE"
    TaskCancel = b"TC"
    TaskCancelEcho = b"TX"
    TaskResult = b"TR"
    Heartbeat = b"HB"
    MonitorRequest = b"MR"
    MonitorResponse = b"MS"

    @staticmethod
    def allowed_values():
        return {member.value for member in MessageType}


class TaskStatus(enum.Enum):
    Success = b"S"
    Failed = b"F"
    Canceled = b"C"


class TaskEchoStatus(enum.Enum):
    OK = b"OK"
    Duplicated = b"DC"
    Failed = b"FD"
