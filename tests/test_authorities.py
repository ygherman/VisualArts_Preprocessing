from unittest import TestCase


class Test(TestCase):
    def test_check_lang(self):
        from VC_collections.AuthorityFiles import Authority_instance

        eng_language_mapping = (
            Authority_instance.df_languages.reset_index()
            .set_index("שם שפה אנגלית")["קוד שפה"]
            .to_dict()
        )
        self.assertTrue(eng_language_mapping["Arabic"], "ara")
