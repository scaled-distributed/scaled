import enum


class MessageType(enum.Enum):
    Task = b"TK"
    TaskEcho = b"TE"
    TaskCancel = b"TC"
    TaskCancelEcho = b"TX"
    TaskResult = b"TR"

    Heartbeat = b"HB"

    FunctionCheck = b"FC"
    FunctionAdd = b"FA"
    FunctionEcho = b"FE"
    FunctionRequest = b"FQ"
    FunctionResponse = b"FP"

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
    SubmitOK = b"SK"
    CancelOK = b"CK"
    Duplicated = b"DC"
