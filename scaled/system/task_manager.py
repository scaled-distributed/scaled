from scaled.system.objects import Task


class TaskManager:
    def __init__(self):
        self.current_task_id = 0
        self.function_map = {}
        self.task_map = {}

    def register_function(self, name: bytes, fn: bytes):
        self.function_map[name] = fn

    def remove_function(self, name: bytes):
        self.function_map.pop(name)

    def create_task(self, source: bytes, function_name: bytes, args: bytes) -> Task:
        self.current_task_id += 1
        task = Task(self.current_task_id, source, function_name, args)
        self.task_map[self.current_task_id] = task
        return task

    def remove_task(self, task_id: int):
        self.task_map.pop(task_id)
