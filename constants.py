import json
import logging
import os
import sys
from logging import config


def configure_logging(path):
    if path and os.path.exists(path):
        with open(path, 'r') as c:
            config_json = json.load(c)
            config.dictConfig(config_json)
    else:
        logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                            stream=sys.stdout,
                            level=logging.DEBUG)
        # set pyspark logger output to ERROR to keep python logs clean
        pyspark_log = logging.getLogger('py4j')
        pyspark_log.setLevel(logging.WARN)


class MissingDataWarning(Warning):
    """Raise for the case in which output was produced but there was no data (all empty) for one of the days processed"""

class NoOutputException(Exception):
    """Raise for the case in which no output was produced because of no data for all days processed"""

class DataScienceWarning(Warning):
    """Raise for non-fatal errors that generate suspicious results"""