from loguru import logger
import pendulum
import sys


def set_datetime(record):
    record["extra"]["datetime"] = pendulum.now("Asia/Hong_Kong").strftime(
        "%Y-%m-%d-%H-%M-%S"
    )


def configure_logger(local_log_path: str, logger_name: str):
    """
    Configure logger for the project. It will force the logger to use the same timezone as Hong Kong.

    :param local_log_path: path to the local log folder
    :return i: index of the log file
    """
    logger.remove()
    logger.configure(patcher=set_datetime)
    i = logger.add(
        f"{local_log_path}/{logger_name}.log",
        format="<green>{extra[datetime]}</green> | <level>{level}</level> | <level>{message}</level>",
    )
    logger.add(
        sys.stderr,
        format="<green>{extra[datetime]}</green> | <level>{level}</level> | <level>{message}</level>",
    )
    return i