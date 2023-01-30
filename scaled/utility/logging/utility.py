import dataclasses
import logging
import logging.config
import typing
import os
import enum


class LogType(enum.Enum):
    Screen = enum.auto()
    File = enum.auto()
    ZMQ = enum.auto()


@dataclasses.dataclass
class LogPath:
    log_type: LogType
    path: str


class LoggingLevel(enum.Enum):
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


def setup_logger(file_names: typing.Tuple[str] = ("/dev/stdout",), logging_level: str = LoggingLevel.INFO.name):
    log_paths = [LogPath(log_type=__detect_log_types(file_name), path=file_name) for file_name in file_names]
    __logging_config(log_paths=log_paths, logging_level=logging_level)
    logging.info(f"logging to {file_names}")


def __detect_log_types(file_name: str) -> LogType:
    if file_name in {"-", "/dev/stdout"}:
        return LogType.Screen

    if file_name.startswith("tcp://"):
        return LogType.ZMQ

    return LogType.File


def __format(name, color=None) -> str:
    if not name:
        return ""

    name = "%({name})s".format(name=name)
    if not color:
        return name

    return __colored(name, color)


def __colored(text, color) -> str:
    return "%({color})s{text}%(reset)s".format(text=text, color=color)


def __generate_log_config() -> typing.Dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,  # this fixes the problem
        "formatters": {
            "colored": {
                "()": "colorlog.ColoredFormatter",
                "format": "[{levelname}]{asctime}: {message}".format(
                    levelname=__format("levelname", color="log_color"),
                    asctime=__format("asctime", color="green"),
                    module=__format("module"),
                    message=__format("message", color="log_color"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
                "reset": True,
                "log_colors": {"DEBG": "white", "INFO": "white", "WARN": "yellow", "EROR": "red", "CTIC": "red"},
            },
            "standard": {
                "format": "[{levelname}]{asctime}: {message}".format(
                    levelname=__format("levelname"),
                    asctime=__format("asctime"),
                    module=__format("module"),
                    message=__format("message"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
            },
            "verbose": {
                "format": "[{levelname}]{asctime}:{module}:{funcName}:{lineno}: {message}".format(
                    levelname=__format("levelname"),
                    asctime=__format("asctime"),
                    module=__format("module"),
                    funcName=__format("funcName"),
                    lineno=__format("lineno"),
                    message=__format("message"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
            },
        },
        "handlers": {},
        "loggers": {"": {"handlers": [], "level": "DEBUG", "propagate": True}},
    }


def __logging_config(log_paths: typing.List[LogPath], logging_level: str = LoggingLevel.INFO.name):
    logging.addLevelName(logging.INFO, "INFO")
    logging.addLevelName(logging.WARNING, "WARN")
    logging.addLevelName(logging.ERROR, "EROR")
    logging.addLevelName(logging.DEBUG, "DEBG")
    logging.addLevelName(logging.CRITICAL, "CTIC")

    config = __generate_log_config()
    handlers = config["handlers"]
    root_loggers = config["loggers"][""]["handlers"]

    for log_path in log_paths:
        if log_path.log_type == LogType.Screen:
            handlers["console"] = __create_stdout_handler(logging_level)
            root_loggers.append("console")
            continue

        elif log_path.log_type == LogType.File:
            handlers[log_path.path] = __create_time_rotating_file_handler(logging_level, log_path.path)
            root_loggers.append(log_path.path)
            continue

        elif log_path.log_type == LogType.ZMQ:
            raise NotImplementedError()

        raise TypeError(f"Unsupported LogPath: {log_path}")

    logging.config.dictConfig(config)


def __create_stdout_handler(logging_level: str):
    return {
        "class": "logging.StreamHandler",
        "level": logging_level,
        "formatter": "colored",
        "stream": "ext://sys.stdout",
    }


def __create_time_rotating_file_handler(logging_level: str, file_path: str):
    return {
        "class": "logging.handlers.TimedRotatingFileHandler",
        "level": logging_level,
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(file_path)),
        "when": "midnight",
    }


def __create_size_rotating_file_handler(log_path) -> typing.Dict:
    return {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "INFO",
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(log_path)),
        "maxBytes": 10485760,
        "backupCount": 20,
        "encoding": "utf8",
    }


def __parse_logging_level(value):
    return LoggingLevel(value).value
