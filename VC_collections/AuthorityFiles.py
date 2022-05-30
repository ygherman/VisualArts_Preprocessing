"""
SYNOPSIS
    TODO helloworld [-h,--help] [-v,--verbose] [--version]

DESCRIPTION
    TODO This describes how to use this script. This docstring
    will be printed by the script if there is an error or
    if the user requests help (-h or --help).
    
PROJECT NAME:
    helper_fuctions

AUTHOR
    Yael Vardina Gherman <Yael.VardinaGherman@nli.org.il>
    Yael Vardina Gherman <gh.gherman@gmail.com>

LICENSE
    This script is in the public domain, free from copyrights or restrictions.

VERSION
    Date: 02/08/2019 00:25
    
    $
"""
import os
import re
from pathlib import Path

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from VC_collections.files import create_df_from_gs

mapper_655_to_999 = {
    "טבלת חישובי שטחים (תכניות ושרטוטים)": "CHART",
    "תמונות נעות": "VIDEO FILE",
    "שמע": "AUDIO FILE",
    "תצלומים": "PHOTOGRAPH",
    "תשריט (תכניות ושרטוטים)": "MAP",
    "מפת מדידה (תכניות ושרטוטים)": "MAP",
    "מפת תכנון (תכניות ושרטוטים)": "MAP",
    "מפה (פרסומים)": "MAP",
}


mapper_999_to_LDR = {}


# def create_df_from_gs(spreadsheet, worksheet):
#     """
#     Function gets name of worksheet from a Google Sheet Spreadsheet, and returns it
#     as a pandas DataFrame
#     :param spreadsheet:
#     :param worksheet: The name of the worksheet
#     :return: df - pandas Dataframe of the worksheet
#              cols - list of column names
#     """
#     # create a dataframe from the given worksheet
#     sheet = spreadsheet.worksheet(worksheet)
#     dict_gs = sheet.get_all_records()
#     headers = dict_gs[0].keys()
#     headers = [header.strip() for header in headers]
#
#     df = pd.DataFrame(dict_gs)
#
#     # remove empty rows
#     df.replace(np.nan, "", inplace=True)
#
#     return df, headers


def order_media_format(df_media_format_auth):
    # drop first row
    # df_media_format_auth = df_media_format_auth.reindex(df_media_format_auth.index.drop(0))
    df_media_format_auth = df_media_format_auth.rename(
        columns={
            "מדיה MEDIA + פורמט": "MEDIA_FORMAT",
            "מדיה": "MEDIA",
            "קוד מונח": "MEDIA_ID",
            "פורמט": "FORMAT",
        }
    )

    df_media_format_auth["TEMP"] = df_media_format_auth.MEDIA_FORMAT.str.split("--")

    # expand df.media_format into its own dataframe
    media_format = df_media_format_auth["TEMP"].apply(pd.Series)

    # rename each variable
    media_format = media_format.rename(columns={0: "MEDIA", 1: "FORMAT"})
    # media_format = media_format.dropna()

    # join the tags dataframe back to the original dataframe
    df_media_format_auth = pd.concat([df_media_format_auth[:], media_format[:]], axis=1)
    # df_media_format_auth = df_media_format_auth.dropna()

    df_media_format_auth = df_media_format_auth.drop("TEMP", 1)

    media_format_auth = df_media_format_auth

    media_format_auth_mapping = media_format_auth[
        ["MARC21 534", "MEDIA_FORMAT", "MARC 338 rdacarrier", "MARC 337 rdamedia"]
    ]
    media_format_mapping_dict = pd.Series(
        media_format_auth_mapping["MARC21 534"].values,
        index=media_format_auth["MEDIA_FORMAT"].values,
    ).to_dict()

    media_format_search_dict = pd.Series(
        media_format_auth["MEDIA_FORMAT"].values,
        index=media_format_auth["MEDIA_FORMAT"].values,
    ).to_dict()

    for key, value in media_format_mapping_dict.items():
        media_format_mapping_dict[key] = value

    return media_format_auth, media_format_mapping_dict, media_format_search_dict


def order_archival_material(df_arch_mat_auth):
    archiv_mat_cols = {
        "סוג חומר": "ARCHIVAL_MATERIAL",
        "מונח סוג חומר רמה 1": "skosxl:broader@prefLabel",
        "מונח סוג חומר רמה 2": "skosxl:prefLabel@lang=heb",
        "הסבר למונח": "skosxl:scopeNote@lang=heb",
        "ערכים לשדות כותרת ותיאור *ערכים אלו אינם מופיעים במקור נתונים": "ARCHIVAL_MATERIAL_ALT_HEB",
        "מדינת הפרסום/צילום": "country_needed",
        "סוג יוצר איש": "creator_pers",
        "סוג יוצר תאגיד": "creator_corp",
    }
    # rename columns names of Archival material table
    df_arch_mat_auth = df_arch_mat_auth.rename(columns=archiv_mat_cols)

    # create a dictionary for mapping
    df_arch_mat_mapping = df_arch_mat_auth.loc[
        df_arch_mat_auth.index, ["ARCHIVAL_MATERIAL", "MARC21 655 7", "rdacontent 336"]
    ]
    arch_mat_mapping_dict = pd.Series(
        df_arch_mat_mapping["MARC21 655 7"].values,
        index=df_arch_mat_mapping.ARCHIVAL_MATERIAL.values,
    ).to_dict()

    df_arch_mat_search = df_arch_mat_auth.loc[
        df_arch_mat_auth.index,
        [
            "skosxl:broader@prefLabel",
            "skosxl:prefLabel@lang=heb",
            "ARCHIVAL_MATERIAL",
            "ARCHIVAL_MATERIAL_ALT_HEB",
            "MARC21 655 7",
            "rdacontent 336",
        ],
    ]
    df_arch_mat_search["clean_value"] = df_arch_mat_search.apply(
        lambda row: row["skosxl:broader@prefLabel"]
        if row["skosxl:prefLabel@lang=heb"] == ""
        else row["skosxl:prefLabel@lang=heb"],
        axis=1,
    )

    df_arch_mat_search["all_terms"] = (
        df_arch_mat_search["clean_value"].map(str)
        + ";"
        + df_arch_mat_search["ARCHIVAL_MATERIAL"]
        + ";"
        + df_arch_mat_search["ARCHIVAL_MATERIAL_ALT_HEB"]
    )

    arch_mat_search_dict = pd.Series(
        df_arch_mat_search["all_terms"].values,
        index=df_arch_mat_search["ARCHIVAL_MATERIAL"].values,
    ).to_dict()

    return (
        df_arch_mat_auth,
        df_arch_mat_search,
        arch_mat_mapping_dict,
        arch_mat_search_dict,
    )


def order_credits(df_credits):
    df_credits["597"] = (
        "$$a" + df_credits["קרדיט עברית"].map(str) + "$$b" + df_credits["קרדיט אנגלית"]
    )
    return df_credits


def create_privacy_mapping_dict(df_privacy_values):
    privacy_mapping_dict = pd.Series(
        df_privacy_values.index.values, index=df_privacy_values.index.values
    ).apply(lambda x: "$$f" + x)

    privacy_search = pd.Series(
        df_privacy_values["רמיזות"].apply(lambda x: x.split(";")),
        index=df_privacy_values.index.values,
    ).to_dict()

    # privacy_search_dict = dict()
    # for key, value in privacy_search.items():
    #     for item in value:
    #         privacy_search_dict[item] = key

    privacy_search_dict = pd.Series(
        df_privacy_values["רמיזות"].values, index=df_privacy_values.index.values
    ).to_dict()

    return privacy_mapping_dict, privacy_search_dict


def create_countries_mapping_dict(df_countries):
    countries_mapping_dict = pd.Series(
        df_countries.index.values, index=df_countries.index.values
    )


class Authority:
    BASE_PATH = Path.cwd()

    def __init__(self):
        # use creds to create a client to interact with the Google Drive API
        scope = ["https://spreadsheets.google.com/feeds"]
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                "google_drive_api/client_secret.json", scope
            )
        except OSError as e:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                r"..\google_drive_api\client_secret.json",
                scope,
            )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(
            "https://docs.google.com/spreadsheets/d/1736sL9unbiOMbcrIYgSkCSvhU2-LCthSLVtYLPSpZ98"
        )

        # fetch all tables
        df_media_format_auth, media_cols = create_df_from_gs(spreadsheet, "מדיה פורמט")
        df_arch_mat_auth, archival_cols = create_df_from_gs(spreadsheet, "סוג חומר")
        df_arch_mat_labels = ["ARCHIVAL_MATERIAL", "MARC21 655 7", "rdacontent 336"]
        df_arch_mat_mapping = df_arch_mat_auth.loc[
            df_arch_mat_auth.index.intersection(df_arch_mat_labels)
        ]
        df_creator_corps_role, corps_role_cols = create_df_from_gs(
            spreadsheet, "סוגי ארגונים-תפקידים"
        )
        df_creator_corps_role = df_creator_corps_role.rename(
            columns={
                "סוגי ארגונים": "CREATORS_CORPS_ROLE",
                "מילות מפתח - סוגי ארגונים בפרוייקט ערבית": "CREATORS_CORPS_ROLE_ARA",
            }
        )
        df_creator_pers_role, pers_role_cols = create_df_from_gs(
            spreadsheet, "סוגי אישים-תפקידים"
        )
        df_creator_pers_role = df_creator_pers_role.rename(
            columns={
                "מילות מפתח - סוגי אישים בפרוייקט": "CREATORS_PERS_ROLE",
                "מילות מפתח - סוגי אישים בפרוייקט ערבית": "CREATORS_PERS_ROLE_ARA",
            }
        )
        df_catalogers, cataloger_cols = create_df_from_gs(spreadsheet, "שם הרושם")
        df_catalogers = df_catalogers.set_index("שם הרושם")
        cataloger_name_mapper = df_catalogers.to_dict()["קיצור אלף"]

        df_countries, countries_cols = create_df_from_gs(spreadsheet, "מדינת פרסום")
        df_countries = df_countries.set_index("מדינת פרסום")
        countries_mapping_dict = pd.Series(
            df_countries.index.values, index=df_countries.index.values
        ).to_dict()

        df_countries_ara = df_countries.set_index("מדינת פרסום בערבית")
        countries_mapping_dict_ara = pd.Series(
            df_countries_ara.index.values, index=df_countries_ara.index.values
        ).to_dict()

        roles_dict = {
            "pers_roles": df_creator_pers_role["CREATORS_PERS_ROLE"].tolist()
            + df_creator_pers_role["CREATORS_PERS_ROLE_ARA"].tolist(),
            "corps_roles": df_creator_corps_role["CREATORS_CORPS_ROLE"].tolist()
            + df_creator_corps_role["CREATORS_CORPS_ROLE_ARA"].tolist(),
        }

        df_languages, languages_cols = create_df_from_gs(spreadsheet, "שפה")
        df_languages = df_languages.set_index("שם שפה עברית")
        language_mapping_dict = pd.Series(
            df_languages.index.values, index=df_languages.index.values
        ).to_dict()

        df_languages_ara = df_languages.loc[df_languages["שם שפה ערבית"] != ""]
        df_languages_ara = df_languages.set_index("שם שפה ערבית")
        language_mapping_dict_ara = pd.Series(
            df_languages_ara.index.values, index=df_languages_ara.index.values
        ).to_dict()

        language_mapping_dict_ara = {
            k: v for k, v in language_mapping_dict_ara.items() if v
        }

        df_credits, credits_col = create_df_from_gs(spreadsheet, "קרדיטים")
        df_credits = df_credits.set_index("סימול הארכיון")

        df_privacy_values, copyright_cols = create_df_from_gs(
            spreadsheet, "מגבלות פרטיות"
        )
        df_privacy_values = df_privacy_values.set_index("מגבלות פרטיות")
        privacy_mapping_dict, privacy_search_dict = create_privacy_mapping_dict(
            df_privacy_values
        )

        df_level, level_cols = create_df_from_gs(spreadsheet, "רמת תיאור")

        df_subjects, df_subjects_cols = create_df_from_gs(spreadsheet, "נושאים")
        df_subjects["650 7_ENG"] = df_subjects["650 7_ENG"].str.replace('\u200f', "")
        df_subjects_mapper = df_subjects.set_index("נושא עברית").to_dict()["650 7_ENG"]

        (
            self.df_media_format_auth,
            self.media_format_mapping_dict,
            self.media_format_search_dict,
        ) = order_media_format(df_media_format_auth)
        (
            self.df_arch_mat_auth,
            self.df_arch_mat_search,
            self.arch_mat_mapping_dict,
            self.arch_mat_search_dict,
        ) = order_archival_material(df_arch_mat_auth)
        self.df_arch_mat_mapping = df_arch_mat_mapping
        self.df_creator_corps_role = df_creator_corps_role
        self.df_creator_pers_role = df_creator_pers_role
        self.df_cataloguers = df_catalogers
        self.cataloger_name_mapper = cataloger_name_mapper
        self.df_countries = df_countries
        self.df_countries_ara = df_countries_ara
        self.countries_mapping_dict = countries_mapping_dict
        self.df_languages = df_languages
        self.language_mapping_dict = language_mapping_dict
        self.language_mapping_dict_ara = language_mapping_dict_ara
        self.roles_dict = roles_dict
        self.df_level = df_level
        self.df_credits = order_credits(df_credits)
        self.df_privacy_values = df_privacy_values
        self.privacy_mapping_dict = privacy_mapping_dict
        self.privacy_search_dict = privacy_search_dict
        self.mapper_655_to_999 = mapper_655_to_999
        self.df_subject_mapper = df_subjects_mapper
        self.countries_mapping_dict_ara = countries_mapping_dict_ara


if __name__ != "__main__":
    Authority_instance = Authority()
