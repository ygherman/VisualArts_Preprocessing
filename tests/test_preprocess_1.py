from unittest import TestCase
from preprocess_1 import *


class Test(TestCase):
    def test_check_unitid(self):

        test_data_with_dup = {
            "UNITID": ["ABCD", "ABCD"]
        }
        self.fail()
