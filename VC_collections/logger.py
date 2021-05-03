import logging
from pathlib import Path


# class VC_Logger(logging.LoggerAdapter):


def show_loggers():
    loggers = [("root", logging.getLogger())]
    for name in sorted(logging.Logger.manager.loggerDict.keys()):
        logger = logging.getLogger(name)
        loggers.append((name, logger))
    for name, logger in loggers:
        indent = ""
        if name != "root":
            indent = "   " * (name.count(".") + 1)
        if logger.propagate:
            prop = "+ "
        else:
            prop = "  "
        handlers = ""
        if len(logger.handlers) > 0:
            handlers = ": " + str(logger.handlers)
        level = logging.getLevelName(logger.level)
        eff_level = logging.getLevelName(logger.getEffectiveLevel())
        if level == eff_level:
            level_str = " [%s]" % level
        else:
            level_str = " [%s -> %s]" % (level, eff_level)
        print(indent + prop + name + level_str + handlers)


# def __init__(self, branch, collection_id):
def initialize_logger(branch, collection_id):
    """
        Set up logging to file and console.
    :param branch:
    :param collection_id:
    """
    # show_loggers()

    reports_path = Path.cwd().parent / branch / collection_id / "Data" / "reports"

    logging.basicConfig(
        level=logging.DEBUG,
        filename=reports_path / (collection_id + ".log"),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%y-%m-%d %H:%M",
    )
    # logging.basicConfig(level=logging.INFO,
    #                     format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    #                     datefmt='%y-%m-%d %H:%M',
    #                     )

    # setup logging file handler
    dt_fmt = "%y-%m-%d %H:%M"
    file_handler = logging.FileHandler(filename=reports_path / (collection_id + ".log"))
    file_handler.setFormatter(
        logging.Formatter(
            "[{levelname}] {asctime}s {name}  {message}", dt_fmt, style="{"
        )
    )

    # setup logging console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(
        logging.Formatter("[{levelname}] {name}  {message}", style="{")
    )

    print(f"Logger has handlers? {logging.getLogger().hasHandlers()}")
    if not logging.getLogger().hasHandlers():
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().addHandler(stream_handler)
    else:
        logging.getLogger().addHandler(stream_handler)
