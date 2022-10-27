from collections import namedtuple

from scaled.system.objects import MessageType


SubmitClass = namedtuple("SubmitTask", ["function_name", "function", "args"])

FRONTEND_PROTOCOL = {
    MessageType.SubmitTask.value: SubmitClass,
}


FunctionRequestClass = namedtuple("FunctionRequest", ["function_name"])
AddFunctionClass = namedtuple("AddFunctionClass", ["function_name", "function"])
DelFunctionClass = namedtuple("DelFunctionObj", ["function_name"])
TaskClass = namedtuple("TaskObj", ["task_id", "function_name", "args"])
TaskResultClass = namedtuple("TaskResultObj", ["task_id", "task_result"])

BACKEND_PROTOCOL = {
    MessageType.FunctionRequest.value: FunctionRequestClass,
    MessageType.FunctionAddInstruction.value: AddFunctionClass,
    MessageType.FunctionDeleteInstruction.value: DelFunctionClass,
    MessageType.Task.value: TaskClass,
    MessageType.TaskResult.value: TaskResultClass,
}
