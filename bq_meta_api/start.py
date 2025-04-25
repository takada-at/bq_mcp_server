from bq_meta_api import config, log


def init_app(log_to_console: bool = True):
    """
    Initialize the FastAPI application.
    """
    log.init_logger(log_to_console)
    config.init_setting()
