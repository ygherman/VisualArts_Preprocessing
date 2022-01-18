from typing import io


from unittest import TestCase, mock
from pathlib import Path

import preprocess_2_on_master

# from unittest.mock import import patch


class Test(TestCase):
    def test_find_collection_id(self):
        test1 = "ArBe"
        test2 = "ArBe-01"
        test3 = "ArBe-01-002"
        test4 = "IL-BATH"
        test5 = "IL-BATH-001"
        test6 = "IL-BATH-001-002"
        self.assertEqual(preprocess_2_on_master.find_collection_id(test1), "ArBe")
        self.assertEqual(preprocess_2_on_master.find_collection_id(test2), "ArBe")
        self.assertEqual(preprocess_2_on_master.find_collection_id(test3), "ArBe")
        self.assertEqual(preprocess_2_on_master.find_collection_id(test4), "IL-BATH")
        self.assertEqual(preprocess_2_on_master.find_collection_id(test5), "IL-BATH")
        self.assertEqual(preprocess_2_on_master.find_collection_id(test6), "IL-BATH")

    def test_create_dataframe(self):
        self.fail()

    @patch("gspread.authorize")
    def test_sheet(self, authorizeMocka):
        sheet = authorizeMocka.return_value.open_by_url.return_value
        type(sheet).sheet1 = mock.PropertyMock(return_value=None)
        with io.StringIO() as 輸出:
            # call_command('顯示全部sheet狀態', stdout=輸出)
            self.assertIn("sheet內底無工作表", 輸出.getvalue())
