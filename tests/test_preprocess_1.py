from unittest import TestCase

import pandas as pd

import preprocess_1
from preprocess_1 import *


class Test(TestCase):
    def test_check_unitid(self):
        test_data_with_dup = {"UNITID": ["ABCD", "ABCD"]}
        self.fail()

    def test_check_against_viaf(self):
        df_test = pd.ExcelFile("Resources/PGdOg_pers_report.xlsx").parse("PERS_report")
        df_result = preprocess_1.check_against_viaf(df_test)
        df_result.to_excel("test_viaf.xlsx")
        self.fail()
