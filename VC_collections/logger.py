import logging
from pathlib import Path


# class VC_Logger(logging.LoggerAdapter):
from VC_collections.files import create_directory


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
    if not branch.startswith("VC-"):
        branch = "VC-" + branch

    BASE_PATH = Path.cwd().parent.absolute() / (branch) / collection_id

    # initialize directory with all folder and sub-folders for the collection
    (
        data_path,
        data_path_raw,
        data_path_processed,
        data_path_reports,
        copyright_path,
        digitization_path,
        authorities_path,
        aleph_custom21_path,
        aleph_manage18_path,
        aleph_custom04_path,
    ) = create_directory("Alma", BASE_PATH)

    reports_path = Path.cwd().parent / branch / collection_id / "Data" / "reports"
    with open(reports_path / (collection_id + ".log"), "a+"):
        pass

    logging.basicConfig(
        level=logging.DEBUG,
        filename=reports_path / (collection_id + ".log"),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%y-%m-%d %H:%M",
    )

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


def initialize_logger_for_master_process2(branch):
    logging.basicConfig(
        level=logging.DEBUG,
        filename="preprocess2.log",
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%y-%m-%d %H:%M",
    )

    # setup logging file handler
    dt_fmt = "%y-%m-%d %H:%M"
    file_handler = logging.FileHandler(filename="preprocess2.log")
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

    logging.getLogger().addHandler(file_handler)
    logging.getLogger().addHandler(stream_handler)
