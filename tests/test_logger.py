import logging
from unittest import TestCase

from VC_collections.logger import initialize_logger


class TestLogger(TestCase):
    def test___init__(self):
        initialize_logger("VC-Architect", "PAlAv")
        logging.info("bla bla")
