import logger.config as config
import os
import json
import copy


class Log:
    def __init__(self, text: str, type: str) -> None:
        self.text = text
        self.type = type

    def __str__(self):
        return f"{self.type.upper() + ' ' if self.type in ('warning', 'error') else ''}{self.text}"


class TaskLogger:
    def __init__(self, task_name: str, task_id: str, date: str) -> None:
        self.task_name = task_name
        self.task_id = task_id
        self.date = date
        self.logs = []
        self.has_errors = False
        self.base_path = f"{config.base_path}/{self.task_name}"
        self.file_name = f"{self.base_path}/{self.date}/{self.date}.log"
        self.metadata = LoggerMetadata(self.task_name, self.date)

    def add_log(self, log: Log, close: bool = False):
        self.logs.append(log)
        if log.type == "error":
            self.has_errors = True
        if close:
            self.close()

    def add_exception_log(self, exception: Exception, type: str, close: bool = False):
        text = str(exception)
        log = Log(text, type)
        self.add_log(log, close)

    def save_file(self):
        logs = "\n\t".join(list(map(lambda log: str(log), self.logs)))
        text = f"{self.task_id}:\n\t{logs}"

        with open(self.file_name, "a") as file:
            file.write(text + "\n")
            file.close()

    def close(self):
        self.save_file()
        result = "error" if self.has_errors else None
        self.metadata.end_task(result=result)


class LoggerMetadata:
    def __init__(
        self, task_name: str, date: str, template: dict = config.metadata_template
    ) -> None:
        self.date = date
        self.path = f"{config.base_path}/{task_name}/{self.date}"
        self.template = template
        self.metadata = None

    def start(self, total_tasks: int) -> None:
        self.metadata = copy.deepcopy(self.template)
        os.makedirs(self.path, exist_ok=True)
        self.metadata["total_tasks"] = total_tasks
        self.serialize()

    def parse(self) -> None:
        with open(self.path + "/.metadata.json", "r") as file:
            metadata = json.load(file)
            self.metadata = metadata

    def serialize(self) -> None:
        with open(self.path + "/.metadata.json", "w") as file:
            json.dump(self.metadata, file, indent=2)

    def start_task(self) -> None:
        self.metadata["started_tasks"] += 1

    def end_task(self, result: str = "success") -> None:
        if result != "error":
            self.metadata["successful_tasks"] += 1
        else:
            self.metadata["failed_tasks"] += 1

        self.metadata["ended_tasks"] += 1

        self.serialize()

        if self.metadata["started_tasks"] == self.metadata["total_tasks"]:
            self.close()

    def close(self):
        result = "\n"

        try:

            result += f"Carga finalizada. Finalizadas {self.metadata['ended_tasks']} de {self.metadata['total_tasks']} tareas.\n"
            result += f"{self.metadata['successful_tasks']} tareas exitosas.\n"
            result += f"{self.metadata['failed_tasks']} tareas con errores.\n"

            with open(self.path + "/" + self.date + ".log", "a") as file:
                file.write(result)
        except Exception:
            return None
