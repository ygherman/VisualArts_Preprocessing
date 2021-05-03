from unittest import TestCase


class Test(TestCase):
    def test_clean_date_format(self):
        from VC_collections.value import clean_date_format

        self.assertEqual(clean_date_format("29-09-2010"), "2010-09-29")
        self.assertEqual(clean_date_format("1950"), "1950")
        self.assertEqual(clean_date_format("1994-07"), "1994-07")
        self.assertEqual(clean_date_format("1994-7-31"), "1994-07-31")
        self.assertEqual(clean_date_format("1964-1"), "1964-01")
        self.assertEqual(clean_date_format("1963-26-12"), "1963-12-26")
