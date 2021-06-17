from unittest import TestCase, main
import pandas as pd


class Test_MARC(TestCase):
    def test_create_MARC_921_933(self):
        test_df = pd.DataFrame(
            {
                "תאריך הרישום": {
                    "997007810681405171": "2020-01-26 15:56",
                    "997007810681305171": "2019-11-25 13:06",
                    "997007810681205171": "2019-11-25 13:41",
                    "997007810681105171": "2019-11-25 16:15",
                    "997007810681005171": "2019-11-25 13:06",
                    "997007810680905171": "2019-11-21 8:59",
                    "997007810680805171": "2019-11-25 15:09",
                    "997007810680705171": "2019-11-25 17:52",
                    "997007810680605171": "2019-11-25 17:53",
                    "997007810680505171": "2019-11-25 17:53",
                    "997007810680405171": "2019-12-18 16:20",
                    "997007810680305171": "2019-12-18 16:24",
                    "997007810680205171": "2019-12-18 16:24",
                    "997007810680105171": "2019-12-18 16:25",
                },
                "שם הרושם": {
                    "997007810681405171": "יובל עציוני;צליל ניב",
                    "997007810681305171": "יובל עציוני",
                    "997007810681205171": "יובל עציוני",
                    "997007810681105171": "יובל עציוני",
                    "997007810681005171": "יובל עציוני",
                    "997007810680905171": "יובל עציוני",
                    "997007810680805171": "יובל עציוני",
                    "997007810680705171": "יובל עציוני",
                    "997007810680605171": "יובל עציוני",
                    "997007810680505171": "יובל עציוני",
                    "997007810680405171": "יובל עציוני",
                    "997007810680305171": "יובל עציוני",
                    "997007810680205171": "יובל עציוני",
                    "997007810680105171": "יובל עציוני",
                },
            }
        )

    def test_check_date_values_in_row(self):
        from VC_collections.marc import extract_years_from_text

        self.assertEqual(extract_years_from_text("1930/1990"), ["1930", "1990"])
        self.assertEqual(extract_years_from_text("[בערך 1940-2018]"), ["1940", "2018"])

    def test_create_marc_999_values_list(self):
        from VC_collections.marc import create_MARC_999_values_list

        list_999_values_test = ["תצלומים", "שמע", "תשריט", "מפת מדידה"]
        self.assertEqual(
            create_MARC_999_values_list(list_999_values_test),
            [
                "$$aAUDIO FILE (AUDIO FILE)",
                "$$aPHOTOGRAPH (PHOTOGRAPH)",
                "$$aMAP (MAP)",
            ],
        )

    def test_create_710_current_owner_val(self):
        from VC_collections.marc import create_710_current_owner_val

        eng = "Goor Archive"
        heb = "ארכיון גור"
        self.assertEqual(
            "$$aארכיון גור$$9heb$$ecurrent owner", create_710_current_owner_val(heb)
        )
        self.assertEqual(
            "$$aGoor Archive$$9lat$$ecurrent owner", create_710_current_owner_val(eng)
        )

    def test_create_marc_655(self):
        xl = pd.ExcelFile("Resources/ArBe_test_data.xlsx")
        test_df = xl.parse("קטלוג")
        test_655 = test_df["סוג חומר"]

    def test_create_marc_942(self):
        from VC_collections.marc import create_MARC_942

        xl = pd.ExcelFile("Resources/ArHb_test_data.xlsx")
        test_df = xl.parse("קטלוג")
        df = create_MARC_942(test_df, "ArHb")

        self.fail()

    def test_create_marc_524(self):
        xl = pd.ExcelFile("Resources/IL-JIPR-test.xlsx")
        test_df = xl.parse("קטלוג")

        self.fail()

    def test_create_citation_eng(self):
        from VC_collections.marc import create_citation_eng

        self.assertEqual(
            create_citation_eng(
                "IL-JIPR", "Jerusalem Institute for Policy Research Archive"
            ),
            "Jerusalem Institute for Policy Research Archive, National Library of Israel, Reference code: IL-JIPR",
        )

    def test_add_marc_role(self):
        from VC_collections.marc import add_MARC_role

        test_data1_pers = "לבנון, אריה [מלחין]"
        test_data2_corps = "תיאטרון מחול ענבל [מפיק-תאגיד]"
        test_data3_works = "אשירה לשבזי"
        self.assertEqual(
            add_MARC_role(test_data1_pers, "heb"), "$$aלבנון, אריה$$9heb$$eמלחין"
        )
        self.assertEqual(
            add_MARC_role(test_data2_corps, "heb"),
            "$$aתיאטרון מחול ענבל$$9heb$$eחברת הפקה",
        )

    def test_name_lang_check(self):
        from VC_collections.marc import name_lang_check

        test_data_works = "אשירה לשבזי"
        self.assertEqual(
            name_lang_check(test_data_works, mode="WORKS"),
            "$$aאשירה לשבזי (יצירה כוריאוגרפית)$$9heb",
        )

    def test_create_marc_952_mul_unknown_creators(self):
        import numpy as np
        from VC_collections.marc import create_MARC_952_mul_unknown_creators

        xl = pd.ExcelFile(r"Resources\mul_unknown_creators.xlsx")
        df_test = xl.parse("Data")
        df_results = xl.parse("Results")
        df_test_result = create_MARC_952_mul_unknown_creators(df_test)
        df_test_result = df_test_result.replace(np.nan, "")
        print(df_test_result)
        self.assertTrue(df_test_result.equals(df_results.replace(np.nan, "")))

    def test_create_246_value(self):
        from VC_collections.marc import create_246_value

        test_246_data = {
            "כותרת מתורגמת": [
                "مخطط تنفيذي شامل لحوض وادي النار",
                "Example Title",
                "דוגמה עברית",
            ]
        }
        test_246_df = pd.DataFrame(data=test_246_data)
        test_text_ARA = "مخطط تنفيذي شامل لحوض وادي النار"
        test_text_ENG = "Example Title"

        self.assertEqual(
            create_246_value(test_text_ENG), "$$iEnglish Title:$$aExample Title$$9lat"
        )
        self.assertEqual(
            create_246_value(test_text_ARA),
            "$$iArabic Title:$$aمخطط تنفيذي شامل لحوض وادي النار$$9ara",
        )

    def test_create_marc_520(self):
        from VC_collections.marc import create_MARC_520

        sample_data = pd.DataFrame(
            {
                "תיאור": ["דוגמה של תיאור", "sample of description"],
                "תקציר": ["דוגמה של תקציר", "example of summary"],
            }
        )

        results = pd.DataFrame(
            {
                "5202": ["$$aדוגמה של תיאור", "$$asample of description"],
                "520": ["$$aדוגמה של תקציר$$9heb", "$$aexample of summary$$9lat"],
            }
        )
        self.assertEqual(create_MARC_520(sample_data), results)

    def test_create_marc_306(self):
        from VC_collections.marc import create_MARC_306

        d_input = {"משך": ['00:33:12']}
        df_input = pd.DataFrame(data=d_input)

        d_output = {"306": ['$$a003312']}
        df_output = pd.DataFrame(data=d_output)

        self.assertTrue(df_output.equals(create_MARC_306(df_input)))

    def test_create_710_current_owner_val(self):
        from VC_collections.marc import create_710_current_owner_val
        current_owner_heb = "בעלים נוכחיים"
        current_owner_lat = "current owner"
        test_string_heb = "דוגמה לבעלים נוכחיים"
        test_string_eng = "example current owner"
        test_string_blank = ""
        self.assertEqual(create_710_current_owner_val(test_string_heb),
                         f"{test_string_heb}$$9heb$$e{current_owner_heb}")
        self.assertEqual(create_710_current_owner_val(test_string_eng),
                         "example current owner$$9lat$$ecurrent owner")
        self.assertEqual(create_710_current_owner_val(""), "")

if __name__ == "__main__":
    main()


