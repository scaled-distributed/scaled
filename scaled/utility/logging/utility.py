import datetime
import inspect
import logging
import logging.config
import typing
import os
import enum

import pytz


class LoggingLevel(enum.Enum):
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


def setup_logger(
    directory: typing.Optional[str] = None,
    name: typing.Optional[str] = None,
    logging_level: str = LoggingLevel.INFO.name,
):
    if directory and directory not in ("-", "/dev/stdout"):
        if not os.path.isdir(directory):
            raise ValueError(f"logging path {directory} has to be directory")

        os.makedirs(directory, exist_ok=True)

        path = os.path.join(directory, f"{name}.{datetime.datetime.now(pytz.UTC)}.log")
    else:
        path = None

    _logging_config(path=path, logging_level=logging_level)
    logging.info(f"logging to \"{path if path else '/dev/stdout'}\"")


def get_caller_location(stack_level: int):
    caller = inspect.getframeinfo(inspect.stack()[stack_level][0])
    return f"{caller.filename}:{caller.lineno}"


def _format(name, color=None) -> str:
    if not name:
        return ""

    name = "%({name})s".format(name=name)
    if not color:
        return name

    return _colored(name, color)


def _colored(text, color) -> str:
    return "%({color})s{text}%(reset)s".format(text=text, color=color)


def _file_handler(log_path) -> typing.Dict:
    return {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "INFO",
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(log_path)),
        "maxBytes": 10485760,
        "backupCount": 20,
        "encoding": "utf8",
    }


def _generate_log_config(logging_level: str) -> typing.Dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,  # this fixes the problem
        "formatters": {
            "colored": {
                "()": "colorlog.ColoredFormatter",
                "format": "[{levelname}]{asctime}: {message}".format(
                    levelname=_format("levelname", color="log_color"),
                    asctime=_format("asctime", color="green"),
                    module=_format("module"),
                    message=_format("message", color="log_color"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
                "reset": True,
                "log_colors": {"DEBG": "white", "INFO": "white", "WARN": "yellow", "EROR": "red", "CTIC": "red",},
            },
            "standard": {
                "format": "[{levelname}]{asctime}: {message}".format(
                    levelname=_format("levelname"),
                    asctime=_format("asctime"),
                    module=_format("module"),
                    message=_format("message"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
            },
            "verbose": {
                "format": "[{levelname}]{asctime}:{module}:{funcName}:{lineno}: {message}".format(
                    levelname=_format("levelname"),
                    asctime=_format("asctime"),
                    module=_format("module"),
                    funcName=_format("funcName"),
                    lineno=_format("lineno"),
                    message=_format("message"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": logging_level,
                "formatter": "colored",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {"": {"handlers": ["console"], "level": "DEBUG", "propagate": True},},
    }


def _logging_config(path: typing.Optional[str], logging_level: str = LoggingLevel.INFO.name):
    logging.addLevelName(logging.INFO, "INFO")
    logging.addLevelName(logging.WARNING, "WARN")
    logging.addLevelName(logging.ERROR, "EROR")
    logging.addLevelName(logging.DEBUG, "DEBG")
    logging.addLevelName(logging.CRITICAL, "CTIC")

    config = _generate_log_config(logging_level)
    if path:
        handlers = config["handlers"]
        handlers["log_file"] = _file_handler(path)

        root_loggers = config["loggers"][""]["handlers"]
        root_loggers.append("log_file")

    logging.config.dictConfig(config)


def _parse_logging_level(value):
    return LoggingLevel(value).value
