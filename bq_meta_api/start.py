from bq_meta_api import log


def init_app(log_to_console: bool = True):
    """
    Initialize the FastAPI application.
    """
    log.init_logger(log_to_console)
    from bq_meta_api import config
    config.init_setting()
