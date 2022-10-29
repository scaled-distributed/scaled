import enum


class MessageType(enum.Enum):
    WorkerHeartbeat = b"IF"
    ClientJobMap = b"JM"
    ClientJobGraph = b"GJ"
    ClientJobEcho = b"CE"
    ClientJobResult = b"CR"
    WorkerFunctionRequest = b"FR"
    WorkerFunctionAdd = b"AF"
    WorkerFunctionDelete = b"DF"
    WorkerTask = b"TK"
    WorkerTaskResult = b"TR"


class TaskStatus(enum.Enum):
    Success = b"S"
    Failed = b"F"
    Canceled = b"C"
