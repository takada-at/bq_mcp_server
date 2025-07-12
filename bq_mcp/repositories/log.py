import logging
from pathlib import Path

from bq_mcp.core.entities import LogSetting

logger: logging.Logger | None = None


def get_logger(log_to_console: bool = True) -> logging.Logger:
    """
    Get the logger instance.
    """
    global logger
    if logger is None:
        init_logger()
    assert logger is not None
    return logger


def init_logger(
    log_to_console: bool = True, enable_file_log: bool = False
) -> LogSetting:
    """
    Initialize the logger with a specific format and level.

    If `log_to_console` is True, logs will be printed to the console.
    If `enable_file_log` is True, logs will be saved to a file.
    """
    global logger
    if logger is None:
        if log_to_console:
            logging.basicConfig(
                level=logging.INFO,
            )
            logger = logging.getLogger("bq_meta_api")
            logger.info("Logger initialized. %s", "Logging to console")
        elif enable_file_log:
            DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            log_dir = Path(Path(__file__).parent / "../../logs").resolve()
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "bq_mcp.log"
            formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger = logging.getLogger("bq_mcp")
            logger.propagate = False
            logger.addHandler(file_handler)
            logger.info("Logger initialized. Logging to file: %s", log_file)
        else:
            logger = logging.getLogger("bq_mcp")
            logger.propagate = False
            logger.disabled = True
            logger.setLevel(logging.INFO)
            logger.info("Logger initialized. No logging output configured.")
    return LogSetting(
        log_to_console=log_to_console, enable_file_logging=enable_file_log
    )
