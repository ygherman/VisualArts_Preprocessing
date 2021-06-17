import logging
import os
import re
import sys
from datetime import datetime
from xml.dom import minidom

import alphabet_detector
import dateutil
import numpy as np
from fuzzywuzzy import process

from VC_collections.AuthorityFiles import *
from VC_collections.authorities import (
    is_corp,
    is_pers,
    find_name,
    find_role,
    check_lang,
)
from VC_collections.authorities import map_role_to_relator
from VC_collections.columns import (
    drop_col_if_exists,
    column_exists,
    remove_duplicate_in_column,
)
from VC_collections.explode import explode_col_to_new_df
from VC_collections.project import get_root_index_and_title, lookup_rosetta_file
from VC_collections.value import (
    clean_text,
    find_nth,
    replace_lst_dict,
    is_multi_value,
    clean_date_format,
)

# ROOTID finder
ROOTID_finder = lambda x: x[: find_nth(x, "-", x.count("-"))] if "-" in x else ""

Authority_instance = Authority()


def create_MARC_initial_008(df):
    df["008"] = "######k###########xx######################d"
    return df


def create_MARC_093(df, collection_id):
    """add prefix '$$a' to the value

    :param df = dataframe

    """
    df["093"] = df["סימול"].apply(lambda x: "$$c" + str(x).strip())

    try:
        collection_name_heb = Authority_instance.df_credits.loc[
            collection_id, "שם הארכיון"
        ]
    except:
        sys.stderr.write(
            f"There is no credit in the credits table for collection {collection_id}"
        )
        collection_name_heb = input(
            f"Please enter the hebrew name of the collection {collection_id}: \n"
        )

    df["093"] = df["093"] + "$$d" + collection_name_heb

    df = drop_col_if_exists(df, "911")
    df = drop_col_if_exists(df, "שם האוסף")

    return df


def create_MARC_535(df):
    """
        converts the ['סימול'] field to MARC21 535 encoded field.
    According to the NLI Aleph-Alma conventions the 535 $a Field has a prefix "VIS" for Visual Culture.

    If field ['מיקום פיזי'] exist in original data, then add another 5351 $b field with

    1st indicator (Custodial role) = 1 - Holder of originals.
    $a - Custodian (NR)
    $b - Postal address (R)

    (https://www.loc.gov/marc/bibliographic/bd535.html)[LOC MARC21 bibliographic guidelines]

    :param df: The original Dataframe
    :return: The Dataframe with the new 535 field

    """
    # df["5351_1"] = df.סימול.apply(lambda x: "$$aVIS " + str(x).strip())

    df["5351"] = df["מיקום פיזי"].apply(
        lambda x: "$$b" + str(x).strip() if str(x).strip() != "" else ""
    )

    return df


def create_MARC_351_LDR(df: pd.DataFrame) -> pd.DataFrame:
    """
        converts the ['רמת תיאור'] field to MARC21 351 $c encoded field.
    Information about the organization and arrangement of a collection of items. A closed list of values defined
    by NLI.

    $c - Hierarchical level (NR)

    Also creates MARC LDR  based on hierarchical level.
            - 00000npd^a22^^^^^^a^4500  - for file and item level records
            - 00000npc^a22^^^^^^^^4500 - for all other levels
    :param df: The original Dataframe
    :return:The Dataframe with the new 351 field
    """

    def define_LDR(hier):

        if hier == "File Record" or hier == "Item Record":
            return "00000npd#a22######a#4500"
        else:
            return "00000npc#a22########4500"

    if column_exists(df, "רמת תיאור"):
        col = "רמת תיאור"
    elif column_exists(df, "רמתתיאור"):
        col = "רמתתיאור"

    df["LDR"] = df[col].apply(define_LDR)
    df["351"] = df[col].apply(lambda x: "$$c" + str(x).strip())
    df = drop_col_if_exists(df, col)

    return df


def create_MARC_245(df):
    """
        converts the ['כותרת'] field to MARC21 24510 $a encoded field.

    1st indicator (Title added entrye) = 1 - Added entry.
    2nd indicator (Nonfiling characters) = 0 - No nonfiling characters

    $a - Title
    $b - secondary title

    :param df: The original dataframe.
    :return The Dataframe with the new 245 field
    """

    col = "כותרת"

    try:
        col
    except NameError:
        print("col variable not defined")
        pass
    else:
        df["24510"] = df[col].apply(
            lambda x: "$$a" + str(x).strip()
            if str(x).strip() != ""
            else print(f"bad header: [{x}")
        )
        df = drop_col_if_exists(df, col)

    if column_exists(df, "כותרתאנגלית"):
        col = "כותרתאנגלית"
    elif column_exists(df, "כותרת אנגלית"):
        col = "כותרת אנגלית"
    else:
        return df

    if column_exists(df, "כותרת משנה"):
        df["כותרת משנה"] = df["כותרת משנה"].apply(
            lambda x: "$$b" + str(x).strip() if str(x).strip().lstrip() != "" else ""
        )
        df["24510"] = df["24510"].astype(str) + df["כותרת משנה"]

    if len(df[df["כותרת ערבית"] != '']):
        col_name_246 = last_index_of_reoccurring_column(df, "24631")
        df[col_name_246] = df["כותרת ערבית"].apply(
            lambda x: f"$$a{str(x)}$$9ara" if str(x).strip().lstrip() != "" else ""
        )

    return df


def create_246_value(title):
    lang = check_lang(title)
    print(f"lang found for [{title}]is: [{lang}]")
    if lang == "ara":
        prefix = "Arabic Title:"
    elif lang == "eng":
        prefix = "English Title:"
    else:
        prefix = input(f"please enter the language of the following title: [{title}]\n")
    val = f"$$i{prefix}$$a{title}$$9{lang}"
    return val


def create_MARC_246(df):
    """
        converts the ['כותרת מתורגמת'] field to MARC21 246 $a encoded field.

    1st indicator (Title added entrye) = 1 - Note, added entry.

    $a - Title

    :param df: The original dataframe.
    :return The Dataframe with the new 245 field
    """

    ad = alphabet_detector.AlphabetDetector()

    if "כותרת מתורגמת" in list(df.columns):

        for index, row in df.iterrows():

            if row["כותרת מתורגמת"] == "":
                continue

            if is_multi_value(row["כותרת מתורגמת"]):
                title_list = row["כותרת מתורגמת"].split(";")
                for title in title_list:
                    val = create_246_value(title)
            else:
                val = create_246_value(row["כותרת מתורגמת"])

            df.loc[index, "24631"] = val

        df = drop_col_if_exists(df, "כותרת מתורגמת")

    return df


def clean_header_row(df):
    """
        Prepare table headers/column names for further processing:
        - remove unnamed columns
        - clean and convert column names to lower case
        - remove spaces and special characters

    :param df: the original Dataframe.
    :return: The Dataframe with cleaned headers.
    """
    if "parent" in list(df.columns) and "סימולאב" in list(df.columns):
        df = drop_col_if_exists(df, "parent")

    # remove columns which names starts with 'unnamed'
    unnamed_cols = [x for x in list(df.columns) if "unnamed" in x]

    for col in unnamed_cols:
        df = drop_col_if_exists(df, col)

    # clean and convert column names to lower case
    column_clean = [clean_text(col) for col in list(df.columns)]
    column_clean = map(lambda x: str(x).strip().lower(), column_clean)
    df.columns = column_clean

    return df


def create_MARC_500(df):
    """
        converts the columns [מספר מיכל] and [קוד תיק ארכיון] and [הערות גלוי למשתמש קצה]  [מילות מפתח_מקומות] and [משך] to
    MARC21 500 encoded field.

    Definition: General information for which a specialized 5XX note field has not been defined.
    Subfield Codes: $a - General note (NR)

    Actions:
    - add prefix '$$a' to all values
    - check if the there a value for [מספר מיכל] - if yes, concat the value to field 500
    - check if the there a value for [קוד תיק ארכיון] - if yes, concat the value to field 500
    - check if the there a value for [הערות גלוי למשתמש קצה] - if yes, concat the value to field 500
    - check if the there a value for [מילות מפתח_מקומות] - if yes, concat the value to field 500


    :param df: The original Dataframe
    :return: The Dataframe with the new 500 field
    """
    for index, row in df.iterrows():
        new_value = "$$a"
        if "מספר מיכל" in list(df.columns.values) and str(row["מספר מיכל"]) != "":
            new_value = new_value + "מספר מיכל: " + str(row["מספר מיכל"]) + "; "
        if "מיכל" in list(df.columns.values) and row["מיכל"] != "":
            new_value = new_value + "מספר מיכל: " + str(row["מיכל"]) + "; "
        if (
            "קוד תיק ארכיון" in list(df.columns.values)
            and str(row["קוד תיק ארכיון"]).replace(".0", "") != ""
        ):
            new_value = new_value + "קוד תיק ארכיון: " + row["קוד תיק ארכיון"] + "; "
        if (
            "הערות גלוי למשתמש קצה" in list(df.columns.values)
            and row["הערות גלוי למשתמש קצה"] != ""
        ):
            new_value = new_value + "הערות: " + row["הערות גלוי למשתמש קצה"] + "; "
        if (
            "מילות מפתח_מקומות" in list(df.columns.values)
            and row["מילות מפתח_מקומות"] != ""
        ):
            new_value = (
                new_value + "מקומות המוזכרים בתיק: " + row["מילות מפתח_מקומות"] + "; "
            )
        if "ברקוד" in list(df.columns.values) and row["ברקוד"] != "":
            new_value = new_value + "ברקוד: " + str(row["ברקוד"]) + "; "

        if new_value == "$$a":
            new_value = ""

        df.loc[index, "500"] = new_value.rstrip()

    return df


def create_MARC_500s_4collection(df):
    df = df.rename(
        columns={
            "היסטוריה ארכיונית": "500_1",
            "תיאור הטיפול באוסף בפרויקט": "500_2",
            "סוג אוסף": "500_3",
            "ביבליוגרפיה ומקורות מידע": "581",
        }
    )
    df["500_1"] = df["500_1"].apply(
        lambda x: "$$aהיסטוריה ארכיונית: " + x if str(x).strip() != "" else ""
    )
    df["500_2"] = df["500_2"].apply(
        lambda x: "$$aתיאור הטיפול באוסף בפרויקט: " + x if str(x).strip() != "" else ""
    )
    df["500_3"] = df["500_3"].apply(
        lambda x: "$$aסוג האוסף: " + str(x).strip() if str(x).strip() != "" else ""
    )
    df = explode_col_to_new_df(df, "581")
    cols_581 = [col for col in list(df.columns) if "581" in str(col)]
    for col_name in cols_581:
        df[col_name] = df[col_name].apply(
            lambda x: "$$a" + str(x).strip() if str(x).strip() != "" else ""
        )

    return df


def construct_942(
    row,
    _a,
):
    ad = alphabet_detector.AlphabetDetector()

    if ad.is_latin(_a):
        _9 = "lat"
    elif ad.is_hebrew(_a):
        _9 = "heb"
    else:
        sys.stderr.write(f"[ERROR] Language not detected for [{_a}]")
    _3 = "סימול מקורי:"
    _z = str(row["סימול מקורי"]).replace("nan", "")
    if _z == "" or _z == np.nan or _z == "nan":
        return f"$$a{_a}$$9{_9}"
    else:
        return f"$$a{_a}$$9{_9}$$3{_3}$$z{_z}"


def create_MARC_942(df, collection_id):
    _a = None

    if "סימול מקורי" in list(df.columns):
        df["9421"] = ""
        collection_index = df[df["סימול"] == collection_id].index[0]

    if "בעלים נוכחי" in list(df.columns):
        if df.loc[collection_index, "בעלים נוכחי"] != "":
            _a = df.loc[collection_index, "בעלים נוכחי"]

    else:
        return df

    if _a is not None:

        for index, row in df.iterrows():
            df.loc[index, "9421"] = construct_942(row, _a)
    return df


def create_MARC_561(df):
    """
        converts the columns [סימול מקורי] to  MARC21 561 encoded field.

    Definition: opy-specific field that contains information concerning the ownership and custodial history o
    f the described materials from the time of their creation to the time of their accessioning,
    including the time at which individual items or groups of items were first brought together in their
    current arrangement or collation.

    Subfield Codes: $a - History (NR)

    :param df: The original Dataframe
    :return: The Dataframe with the new 561 field
    """
    if "סימול מקורי" in list(df.columns.values):
        df["561"] = df["סימול מקורי"].apply(
            lambda x: "$$aסימול מקורי: " + str(x) if str(x) != "" else ""
        )
        df = drop_col_if_exists(df, "סימול מקורי")
    return df


def first_creator(x):
    """
    helper function for creating 100/700 and 110/710 distinction.
    search for first creator (first value until semicolon)

    :param x: a string containing the names and roles of all the creators, separated by semicolons
    :return x: the first value in list

    """
    if ";" in x:
        new_x = x[: x.find(";")]
        if new_x == "[]":
            return x.split(";")[1]
        return new_x
    else:
        return x


def all_rest_creators(x):
    """
    helper function for creating 100/700 and 110/710 distinction.
    search for all rest of the creators

    :param x: a string containing the names and roles of all the creators, separated by semicolons
    :return x: the list without the first creator occurance
    """
    if ";" in x:
        return x[x.find(";") + 1 :]
    else:
        return ""


def add_MARC_role(val, lang):
    role = find_role(val)

    if role != "":
        if is_pers(val, Authority_instance.df_creator_pers_role):
            role = map_role_to_relator(
                find_role(val), Authority_instance.df_creator_pers_role, lang
            )
        elif is_corp(val, Authority_instance.df_creator_corps_role):
            role = map_role_to_relator(
                find_role(val),
                Authority_instance.df_creator_corps_role,
                lang,
                mode="CORPS",
            )
        else:
            print("role not found: ", find_role(val))
            role = ""

    if role == "" or role is None:
        val = "$$a" + find_name(val) + "$$9" + lang

    else:
        val = "$$a" + find_name(val) + "$$9" + lang + "$$e" + role

    if val == "$$a$$9heb":
        return ""
    return val


def name_lang_check(
    val, mode="PERS", branch=None
):  # TODO choreographc works - need to add branch parameter
    """
        Checks the language of a given value (mostly English and Hebrew), in order to add the $e subfield for
    language encoded information for the 1XX/6XX/7XX MARC21 Fields.

    :param mode: is searching language for person or corporation. Defualt set to PERS
    :param: val - the string to check the laguage
    :return: new string for creator, exchanging adding '$$e' for
        spaces and $$9 for language
    :type val: string
    """

    ad = alphabet_detector.AlphabetDetector()

    val = str(val)
    val = val.strip()

    if val == "" or val == np.nan:
        return ""

    # find alphabet - if there is more that one default it's hebrew
    if len(ad.detect_alphabet(find_name(val))) > 1:
        lang = "heb"
    elif len(ad.detect_alphabet(find_name(val))) < 1:
        lang = "heb"
    else:
        lang = ad.detect_alphabet(find_name(val)).pop()[:3].lower()

    if mode == "PERS" or mode == "CORPS":
        val = add_MARC_role(val, lang)

    elif mode == "WORKS" and branch == "Dance":
        if lang == "heb":
            val = "$$a" + val + " (יצירה כוריאוגרפית)" + "$$9heb"
        else:
            val = f"$$a{val} (Choreographic Work)$$9{lang}"
    else:
        val = f"$$a{val}$$9{lang}"

    return val


def aleph_creators(df, col_name, mode="PERS", branch=None, with_relators=True):
    """
    fuction's input is the entire table as a dataframe and
        the column name which contains the creators
    :param df: the entire table
    :param col_name: the column names which contain the cretors
    :param mode: whether we are looking for person roles or corporate body roles
    :return: the new data frame with the split columns for first
        creator and rest of creators
        @param df:
        @param col_name:
        @param mode:
        @param branch:
    """
    for index, row in df.iterrows():
        new_creators = list()
        if row[col_name] is None or row[col_name] == "":
            continue
        elif ";" in str(row[col_name]):
            creators = row[col_name].split(";")
        else:
            creators = [str(row[col_name])]
        for creator in creators:
            if find_name(creators[0]) == "לא ידוע" or find_name(creators[0]) == "ריבוי":
                continue
            elif with_relators:
                new_creators.append(creator)
            else:
                new_creators.append(find_name(creator))

        new_creators = [
            name_lang_check(creator, mode, branch) for creator in new_creators
        ]
        df.loc[index, col_name] = ";".join(new_creators)

    df = remove_duplicate_in_column(df, col_name)
    return df


def remove_first_creator_from_700(df):
    for index, row in df.iterrows():
        if is_corp(row["יוצר_ראשון"], Authority_instance.df_creator_corps_role):
            df.loc[index, "יוצרים מוסדות"] = row["יוצרים מוסדות"].replace(
                row["יוצר_ראשון"], ""
            )
        if is_pers(row["יוצר_ראשון"], Authority_instance.df_creator_pers_role):
            df.loc[index, "יוצרים אישים"] = row["יוצרים אישים"].replace(
                row["יוצר_ראשון"], ""
            )
    return df


def find_unknown_multiple_in_column(row: pd.Series, col_name: str):
    multiple_creators = []
    unknown_roles = []
    new_creators = []
    if ";" in row[col_name]:
        creators = row[col_name].split(";")
    else:
        creators = [str(row[col_name]).strip()]
    for creator in creators:
        creator = creator.strip()
        if "ריבוי" in find_name(creator):
            multiple_creators.append(find_role(creator))
        elif "לא ידוע" in find_name(creator):
            unknown_roles.append(find_role(creator))
        elif creator == "":
            continue
        else:
            new_creators.append(creator)
    return multiple_creators, unknown_roles, new_creators


def create_MARC_952_mul_unknown_creators(df):
    # TODO Add check if 1xx is empty then 245 needs to change from first indicator 1 to 0 - Where?

    df["952_g"] = ""

    for index, row in df.iterrows():
        field_952g = "$$g"

        (
            multiple_creators,
            unknown_roles,
            new_creators,
        ) = find_unknown_multiple_in_column(row, "יוצרים")
        # multiple_creators_pers, unknown_roles_pers, new_creators_pers = find_unknown_multiple_in_column(row,
        #                                                                                                 "יוצרים אישים")

        for creator in row["יוצרים"].split(";"):
            if "ריבוי" in creator or "לא ידוע" in creator:
                first_creator_val = ""
                continue
            else:
                first_creator_val = creator
                break

        # multiple_creators = multiple_creators_corps + multiple_creators_pers
        # unknown_roles = unknown_roles_corps + unknown_roles_pers

        # new_creators = new_creators_corps + new_creators_pers
        try:
            if first_creator_val != "":
                new_creators.insert(0, first_creator_val)
                new_creators = list(dict.fromkeys(new_creators))
        except Exception as e:
            sys.stderr.write(f"Exception occured: {e}")

        if len(new_creators) == 1 and new_creators[0] == "":
            if len(unknown_roles) == 0:
                sys.stderr.write(
                    f"Error - problem with creators in index: {index}, unitid: {row['סימול']}"
                )
            else:
                df.loc[index, "יוצרים"] = ""
        else:
            df.loc[index, "יוצרים"] = ";".join(new_creators)

        # if len(new_creators_pers) >= 1 and new_creators_pers[0] != '':
        #     df.loc[index, "יוצרים אישים"] = ";".join(new_creators_pers)
        #
        # if len(new_creators_corps) >= 1 and new_creators_corps[0] != '':
        #     df.loc[index, "יוצרים אישים"] = ";".join(new_creators_pers)

        if len(new_creators) > 1:
            df.loc[index, "יוצרים"] = ";".join(new_creators)

        if len(multiple_creators) > 0:
            field_952g += "Not all creators are cataloged; "
        if len(unknown_roles) > 0:
            field_952g += "Creator undetermined: " + ", ".join(unknown_roles)
        if len(field_952g) > 3:
            df.loc[index, "952_g"] = field_952g
    return df


def create_MARC_100_110(df):
    """
    create column for first creator
    check if creator is a person of a corporate body - against the CVOC of creators roles,
    and insert the value in the respective colomn

    100 - Main Entry-Personal Name (NR)
    -----------------------------------
    Definition:
    Personal name used as a main entry in a bibliographic record. Main entry is assigned according to various
    cataloging rules, usually to the person chiefly responsible for the work.

    1st indicator (Title added entrye) = 1 - Surname

    Subfield Codes:
    $a - Personal name (NR)
    $e - Relator term (R)
    $9 - language code

    110 - Main Entry-Corporate Name (NR)
    ------------------------------------
    Definition:
    Corporate name used as a main entry in a bibliographic record. According to various cataloging rules,
    main entry under corporate name is assigned to works that represent the collective thought of a body.

    1st indicator (Type of corporate name entry element) = 2 - Name in direct order

    Subfield Codes:
    $a - Corporate name or jurisdiction name as entry element (NR)
    $e - Relator term (R)
    $9 - language code

    :param df:
    :return: the modified dataframe with the new 100 and 110 MARC21 encoded fields
    """

    df["1001"] = ""
    df["1102"] = ""

    # create column for first creator
    df["יוצר_ראשון"] = df.יוצרים.apply(lambda x: first_creator(str(x).strip()))
    df = remove_first_creator_from_700(df)
    df["יוצרים"] = df["יוצרים"].apply(all_rest_creators)
    df = df.replace(np.nan, "")

    # check if first creator is a person or a corporate body
    for index, row in df["יוצר_ראשון"].iteritems():
        if is_corp(row, Authority_instance.df_creator_corps_role):
            df.loc[index, "1102"] = row
        elif is_pers(row, Authority_instance.df_creator_pers_role):
            df.loc[index, "1001"] = row

    df = df.replace(np.nan, "")
    df = drop_col_if_exists(df, "יוצר_ראשון")
    df["1001"] = df["1001"].apply(name_lang_check)
    df["1102"] = df["1102"].apply(name_lang_check)

    return df


def create_710_current_owner_val(x):
    if x == "":
        return ""
    ad = alphabet_detector.AlphabetDetector()
    if ad.is_latin(x):
        return x.rstrip() + "$$9lat$$ecurrent owner"
    if ad.is_hebrew(x):
        return x.rstrip() + "$$9heb$$eבעלים נוכחיים"

    sys.stderr.write(
        f"[710] current owner - problem with: {x} - didn't recognize script!"
    )

    return x


# def create_MARC_710_current_owner(df):
#    logger = logging.Logger(__name__)
#
#     if "בעלים נוכחי" in list(df.columns):
#         current_owner_val =
#
#         current_owner_val = current_owner_val.split(";")
#         current_owner_val = [
#             "$$a" + create_710_current_owner_val(x)
#             for x in current_owner_val
#             if x != ""
#         ]
#         if len(current_owner_val) > 1:
#             current_owner_val = ";".join(current_owner_val)
#         else:
#             current_owner_val = "".join(current_owner_val)
#         df["7102"] = current_owner_val
#
#         # df = drop_col_if_exists(df, "בעלים נוכחי")
#     else:
#         logger.info(
#             "[710 current owner] Current owner column doesn't exist.\n"
#             "Getting current owner from Credits table."
#         )
#
#         if (
#             df_credits.loc[collection.collection_id, "מיקום הפקדה עבור בעלים נוכחי"]
#             != ""
#         ):
#             df["בעלים נוכחי"] = df_credits.loc[
#                 collection.collection_id, "מיקום הפקדה עבור בעלים נוכחי"
#             ]
#             df["7102"] = (
#                 df["7102"].astype(str)
#                 + ";"
#                 + df["בעלים נוכחי"].apply(
#                     lambda x: "$$a" + create_710_current_owner_val(x) if x != "" else ""
#                 )
#             )
#         else:
#             logger.error(f"No מיקום הפקדה for collection: {collection.collection_id}")
#
#     return df


def create_MARC_700_710(df):
    """
    create new column in dataframe for all the rest of creators
    :param: df: the dataframe
    """

    df["7001"] = df["יוצרים אישים"]
    df["7102"] = df["יוצרים מוסדות"]

    df = project_photographer(df)

    df = aleph_creators(df, "7001")
    df = aleph_creators(df, "7102")

    df["7001"] = df["7001"].astype("str")
    df["7102"] = df["7102"].astype("str")

    df = remove_duplicate_in_column(df, "7001")
    df = remove_duplicate_in_column(df, "7102")

    current_owner = "$$a" + create_710_current_owner_val(df["בעלים נוכחי"].tolist()[0])

    # check there are no duplicates in 100 and 700
    for index, row in df.iterrows():
        lst_7001 = row["7001"].split(";")
        lst_7102 = row["7102"].split(";")

        if row["1001"] in lst_7001 and row["1001"] != "":
            print("100", row["1001"], "is in 700", lst_7001)
            lst_7001.remove(row["1001"])

        if row["1102"] in lst_7102 and row["1102"] != "":
            print("110", row["1102"], "is in 710", lst_7102)
            lst_7102.remove(row["1102"])

        val_700 = ";$$a".join(list(filter(None, lst_7001)))
        val_710 = ";$$a".join(list(filter(None, lst_7102)))
        val_700 = val_700.replace("$$a$$a", "$$a")
        val_710 = val_710.replace("$$a$$a", "$$a")

        if val_700 != "$$a":
            df.loc[index, "7001"] = val_700

        if val_710 != "$$a":
            df.loc[index, "7102"] = val_710

    df = remove_duplicate_in_column(df, "7001")
    df = remove_duplicate_in_column(df, "7102")

    # adding current owner to columns 710
    if current_owner != "$$a":
        df["7102"] = current_owner + ";" + df["7102"].astype(str)

    df = explode_col_to_new_df(df, "7001")
    df = explode_col_to_new_df(df, "7102")

    df = drop_col_if_exists(df, "7102_current_owner")
    df = drop_col_if_exists(df, "7001")
    df = drop_col_if_exists(df, "7102")

    return df


def construct_MARC_300(words_list):
    nums = ""
    text = ""
    for word in words_list:
        if word[0].isdigit():
            nums += word
        else:
            text += word + " "
    if text != "":
        text = "$$f" + text.rstrip()
    if nums != "":
        nums = "$$a" + nums.rstrip()

    return nums + text


def split_MARC_300(row):
    val_300 = ""
    if str(row) == "":
        return val_300
    elif is_multi_value(str(row)):
        text = str(row).split(";")
        for val in text:
            words = val.split()
            val_300 += construct_MARC_300(words) + ";"
    else:
        words = str(row).split()
        val_300 = construct_MARC_300(words) + ";"

    if val_300.strip() == ";":
        return ""
    else:
        return val_300.rstrip(";")


def create_MARC_300(df):
    """
        converts the ['רמת תיאור'] field to MARC21 351 $c encoded field.
    Information about the organization and arrangement of a collection of items. A closed list of values defined
    by NLI.

    $c - Hierarchical level (NR)

    :param df: The original Dataframe
    :return:The Dataframe with the new 351 field
    """

    df["300"] = df["היקף החומר"].apply(split_MARC_300)
    df = remove_duplicate_in_column(df, "300")
    df = explode_col_to_new_df(df, "300")
    df = drop_col_if_exists(df, "300")

    return df


def create_MARC_306(df):
    """
        converts the ['משך'] field to MARC21 306 $a encoded field.
    Six numeric characters, in the pattern hhmmss, that represent the playing time for a sound recording,
    videorecording, etc..
    Playing time may also be recorded in natural language in a note (field 500)

    $a - Playing time
        Repeatable to allow the recording of the playing time of two or more parts.

    :param df: The original Dataframe
    :return:The Dataframe with the new 306 field
    """
    if column_exists(df, "משך"):
        df["306"] = df["משך"].str.replace(":", "").apply(lambda x: str(x).strip() if x != "" else '')
        df["306"] = df["306"].apply(lambda x: "$$a" + x if x != '' else '')
        df = drop_col_if_exists(df, "משך")
    return df


def check_values_arch_mat(df, arch_mat_col, arch_mat_mapping_dict):
    arch_test = df[arch_mat_col].tolist()
    new_arch = ";".join(arch_test)
    new_arch = list(set(new_arch.split(";")))
    new_arch = list(filter(None, new_arch))  # fastest

    error_values = list()

    for item in new_arch:
        best, score = process.extractOne(item, list(arch_mat_mapping_dict.keys()))
        #     print(item, 'best choice:', best, item == best)
        if best == item:
            continue
        else:
            error_values.append(item)
    #             raise Exception("{} is not in archival material controlled vocabulary".format(item))
    return error_values


def create_MARC_default_copyright(df):
    df["952_default_copyright"] = (
        "$$aCopyright status not determined; No contract"
        + "$$bNo copyright analysis"
        + "$$cYael Gherman {}".format(datetime.today().strftime("%Y%m%d"))
        + "$$dללא ניתוח מצב זכויות"
    )

    df["952"] = df["952_default_copyright"].astype("str") + df["952_g"]
    df = drop_col_if_exists(df, "952_default_copyright")
    df = drop_col_if_exists(df, "952_g")

    df["939"] = (
        "$$aאיסור העתקה"
        + "$$uhttp://web.nli.org.il/sites/NLI/Hebrew/library/items-terms-of-use/Pages/nli-copying-prohibited.aspx"
    )
    df["903"] = "$$aStaff only$$b000000018"

    return df


def create_MARC_999_values_list(lst_655_7: list) -> list:
    lst_999 = []
    for term in lst_655_7:
        if term in Authority_instance.mapper_655_to_999.keys():
            lst_999.append("$$a" + Authority_instance.mapper_655_to_999[term])

    return list(set(lst_999))


def create_MARC_655(df):
    """
        [סוג חומר] column

    All archival material concepts (in hebrew) in the The Visual Arts project Authority file for Archival Material have
     been mapped and aligned to Getty's Art and Architecture Thesaurus (AAT)

    Since all of our concepts are aligned to AAT, the indicators of the 655 field are:
    2nd indicator = 7 - Source specified in subfield $2 (aat, tgm, and so on) if the value is aligned to a known controlled vocabulary
    2nd indicator = 7 - Source = "local" specified in subfield $2 if there is no mapping to a known controlled vocabulary.

    subfields
    $a - Genre/form data or focus term
    $0 - Authority record control number or standard number - we use the direct URI of the Getty's AAT Linked Data
    $2 - Source of term - for this data always AAT
    :param df:
    :return df: the modified df
    """

    if column_exists(df, "סוגחומר"):
        col = "סוגחומר"
    elif column_exists(df, "סוגהחומר"):
        col = "סוגהחומר"
    elif column_exists(df, "סוג החומר"):
        col = "סוג החומר"
    elif column_exists(df, "סוג חומר"):
        col = "סוגהחומר"
    try:
        col
    except NameError:
        sys.stderr.write(f"col variable not defined")
        pass
    else:
        arch_mat_col = process.extractOne(col, list(df.columns))[0]

        for index, row in df.iterrows():
            lst_655_7 = row[arch_mat_col].split(";")
            # print(index, '\n', 'before:', lst_655_7)
            lst_655_7 = list(map(str.strip, lst_655_7))
            broader_terms = list()
            final = list()

            # make list of broader ARCHIVAL_MATERIAL terms
            for term in lst_655_7:
                if ")" in term:
                    try:

                        broader_terms.append(re.findall(r"\((.*)\)", term)[0])
                    except:
                        sys.stderr.write(
                            f"[Error] There is a problem with 655 term: {term}"
                        )
            for term in broader_terms:
                final.append(term.strip())
            final = list(set(final))
            lst_655_7 = lst_655_7 + final
            list_999 = create_MARC_999_values_list(lst_655_7)
            df.loc[index, "999"] = ";".join(list_999)
            # print("before: ", lst_655_7)
            lst_655_7 = replace_lst_dict(
                lst_655_7, Authority_instance.arch_mat_mapping_dict
            )
            # print("after:", lst_655_7)

            df.loc[index, "655 7"] = ";".join(lst_655_7)

        df["655 7"] = df["655 7"].str.replace("$$a$$a", "$$a")

        df = remove_duplicate_in_column(df, "655 7")
        df = explode_col_to_new_df(df, "999", start=3)

        df = explode_col_to_new_df(df, "655 7")
        df = drop_col_if_exists(df, "655 7")
    return df


def project_photographer(df):
    for index, row in df.iterrows():
        if "צלם פרויקט" in row["7001"]:
            val = df.loc[index, "7001"]
            val = val.replace("צלם פרויקט", "צלם")
            # update 5420
            df.loc[index, "5420"] = "$$dNational Library of Israel$$dהספריה הלאומית"
            df.loc[index, "7001"] = val
        if "צלמת פרויקט" in row["7001"]:
            val = df.loc[index, "7001"]
            val = val.replace("צלמת פרויקט", "צלם")
            # update 5420
            df.loc[index, "5420"] = "$$dNational Library of Israel$$dהספריה הלאומית"
            df.loc[index, "7001"] = val

    return df


# TODO change the 506 to the new fields.
def create_MARC_506_post_copyright(df, cols):
    """
    fuction's input is the entire table as a dataframe and constructs the 506 field according to the POST_COPYRIGHT
    file.

    :param cols:
    :param df: the entire table
    :return: the new data frame with the new MARC 506 encoded Field
    """
    if "d" in cols[0]:
        field_506d = cols[0]
        field_506 = cols[1]
    else:
        field_506d = cols[1]
        field_506 = cols[0]

    df[field_506d] = df[field_506d].apply(
        lambda x: "$$d" + str(x).strip() if str(x).strip() != "" else ""
    )
    df["506"] = df[field_506] + df[field_506d]

    df = drop_col_if_exists(df, field_506d)
    df = drop_col_if_exists(df, field_506)

    return df


def create_MARC_041(df):
    """
    fuction's input is the entire table as a dataframe and constructs the 041 field according to the [שפה] column.

    :param df: the entire table
    :return: the new data frame with the new MARC 041 encoded Field
    """
    col = "שפה"
    # TODO if column שפה is empty, then it is dropped. Why?
    try:
        col
    except NameError:
        print("col variable not defined")
        pass
    else:
        language_mapper = Authority_instance.df_languages.to_dict()

        for index, row in df.iterrows():
            if row["שפה"] == "":
                # if row['סוג חומר'] ==
                continue
            languages = row["שפה"].split(";")
            try:
                new_lang = [
                    "$$a" + language_mapper["קוד שפה"][k]
                    for k in languages
                    if len(languages) > 0
                ]
                df.loc[index, "041"] = "".join(new_lang)
            except:
                print("problem with ", index)
                sys.stderr.write(f"problem with languages in {index}")
            field_008 = list(row["008"])

            # insert MARC langauge code in positions 35-37
            for i in range(35, 38):
                field_008[i] = new_lang[0][i - 32]

            df.loc[index, "008"] = "".join(field_008)

        # df = drop_col_if_exists(df, 'שפה')

    return df


# TODO change the 542    to the new fields.
def create_MARC_542_post_copyright(df, col):
    """
    fuction's input is the entire table as a dataframe and constructs the 542 field according to the POST_COPYRIGHT
    file.

    :param col:
    :param df: the entire table
    :return: the new data frame with the new MARC 542 encoded Field
    """

    df = df.rename(columns={col[0]: "542"})

    return df


def correct_506_privacy(df):
    return df


def create_MARC_952(df):
    """
    fuction's input is the entire table as a dataframe and constructs the 540 MARC field.
    Close list of values:
        אין מגבלות פרטיות
        פרטיות-הסכמים וחוזים
        פרטיות-נתונים אישיים
        פרטיות-צנעת הפרט
        פרטיות-מידע רפואי
        פרטיות-אחר


    :param df: the entire table
    :return: the new data frame with the new MARC 952$f encoded Field
    """

    if column_exists(df, "מגבלות פרטיות"):

        for index, row in df.iterrows():
            if ";" in str(row["מגבלות פרטיות"]):
                privacy_list = row["מגבלות פרטיות"].split(";")
                privacy_list = ["$$f" + item for item in privacy_list if item != ""]
                df.loc[index, "מגבלות פרטיות"] = "".join(privacy_list)
            elif row["מגבלות פרטיות"] != "":
                df.loc[index, "מגבלות פרטיות"] = "$$f" + row["מגבלות פרטיות"]
            else:
                continue

        df["952"] = df["952"].astype(str) + df["מגבלות פרטיות"]

    df = drop_col_if_exists(df, "מגבלות פרטיות")
    return df


# TODO


def extract_years_from_text(date_text):
    years = re.findall(r"(\d{4})", str(date_text))
    years = sorted([year for year in years])

    if len(years) < 2:
        return None
    else:
        return years


def check_date_values_in_row(date_start, date_end, date_free_text, index):
    if date_start != "" and date_end != "":
        return date_start, date_end
    elif date_free_text != "":
        date_text = date_free_text
    else:
        sys.stderr.write(
            f"[DATE] Problem with date columns at index: {index} - please check!"
        )
        sys.exit()

    years = extract_years_from_text(date_text)
    if years is None:
        return None, None
    elif len(years) <= 1:
        return None, None
    else:
        return years[0], years[1]


def map_countries(countries_list: str) -> (list, str):
    """

    @param countries_list:
    """
    countries_code_mapper = Authority_instance.df_countries.to_dict()["MARC"]

    countries = countries_list.split(";")
    countries = list(filter(None, countries))
    field_008_country = None
    first_country = None
    if len(countries) > 0:
        try:
            field_008_country = [
                "$$a" + countries_code_mapper[k] for k in countries if len(k) > 0
            ]
        except KeyError as e:
            sys.stderr.write(f"This is not a country {e}! (please correct")
            sys.exit()
        if len(field_008_country) > 1:
            first_country = "vp#"
        else:
            first_country = field_008_country[0][3:]
        if len(first_country) == 2:
            first_country += "#"
    else:
        first_country = "xx#"
    return field_008_country, first_country


def create_MARC_260_044_008_countries(df, country_col):
    for index, row in df.iterrows():
        if row[country_col] != "" or row[country_col] != "":

            # update 008 country
            field_008_country, first_country = map_countries(row[country_col])
            countries = [
                "$$e[" + x.strip() + "]" if x != "" else ""
                for x in row[country_col].split(";")
            ]

            value_008 = df.loc[index, "008"]
            df.loc[index, "008"] = value_008[:15] + first_country + value_008[18:]

            # update 260  country
            df.loc[index, "260"] = df.loc[index, "260"] + "".join(countries)

            # update 044 country code
            if field_008_country is not None:
                df.loc[index, "044"] = "".join(field_008_country)

        else:
            df.loc[
                index, "044"
            ] = "$$axx#"  # code xx# is xx# No place, unknown, or undetermined

    df = remove_duplicate_in_column(df, "044")
    return df


def create_MARC_260_008_date(df, start_date_col, end_date_col, text_date_col):
    """
    fuction's input is the entire table as a dataframe and constructs the 260 field according to the POST_COPYRIGHT
    file.

    Information relating to the publication, printing, distribution, issue, release, or production of a work.
    $a - Place of publication, distribution, etc. (R)
    $e - Place of manufacture (R)
    $g - Date of manufacture (R)

    :param text_date_col:
    :param end_date_col:
    :param start_date_col:
    :param df: the entire table
    :return: the new data frame with the new MARC 008 encoded Field
    """

    logger = logging.getLogger(__name__)

    df[start_date_col] = (
        df[start_date_col]
        .astype(str)
        .replace(r"\.0$", "", regex=True)
        .apply(clean_date_format)
    )
    df[end_date_col] = (
        df[end_date_col]
        .astype(str)
        .replace(r"\.0$", "", regex=True)
        .apply(clean_date_format)
    )

    """
    **************************************************************************************************** 
    the usage of only the year (first 4 digits will be kept only until we figure out what to do with all 
    the date, and in which MARC field the full notmalized -date (YYYY-MM-DD) can be recorded
    ****************************************************************************************************
    """
    df[start_date_col] = df[start_date_col].apply(lambda x: x[:4])
    df[end_date_col] = df[end_date_col].apply(lambda x: x[:4])
    df["260"] = df[text_date_col].apply(lambda x: "$$g" + str(x) if str(x) != "" else "")

    # update 008 field
    for index, row in df.iterrows():

        early_date, late_date = check_date_values_in_row(
            row[start_date_col], row[end_date_col], row[text_date_col], index
        )
        if early_date is None or late_date is None:
            sys.stderr.write(
                f"[DATE] Error with date - check MMS ID {index}, record call number {df.loc[index, 'סימול']}"
            )

        date = f"$$c{early_date}-{late_date}"
        try:
            df.loc[index, "260"] = row["260"] + date
        except Exception as e:
            print(e)

        df.loc[index, "008"] = row["008"][:7] + str(early_date) + str(late_date) + row["008"][15:]

    return df


def create_MARC_520(df):
    """
        converts the ['תיאור'] field to MARC21 520 $a encoded field.

    Subfield Codes
    $a - Summary, etc. (NR)

    :param df: The original dataframe.
    :return The Dataframe with the new 520 field
    """

    df["5202"] = df["תיאור"].apply(
        lambda x: "$$a" + str(x).strip().replace("\n", " ") if str(x) != "" else ""
    )

    df["520"] = df["תקציר"].apply(
        lambda x: "$$a" + str(x).strip().replace("\n", " ") + "$$9" + check_lang(x)
        if str(x) != ""
        else ""
    )
    df = drop_col_if_exists(df, "תיאור")
    df = drop_col_if_exists(df, "תקציר")

    return df


def get_cms_sid(custom04_path, collectionID, df, CMS):
    """
        Fumctions takes the custom04 that maps the call number and the Aleph systme number,
    creates a mapping dataframe (df_aleph) and adds a column system number to the original dataframe.
    :param CMS:
    :type df: Dataframe
    :type custom04_path: Path
    :param custom04_path:
    :param collectionID:
    :param df: the original dataframe
    :return: Two dataframes:
                1. df_aleph - the dataframe that  maps the call number (911) to Aleph system number.
                2. df - the dataframe with system number column
    """
    sysno_file = custom04_path / (collectionID + "_{}_sysno.xlsx".format(CMS))

    assert os.path.isfile(sysno_file), "There is no such File: sysno_file"

    # parse sysno file
    xl2 = pd.ExcelFile(sysno_file)
    if CMS == "aleph":
        df_aleph = xl2.parse("Sheet1")
    else:
        df_aleph = xl2.parse("results")

    # rename columns
    df_aleph = df_aleph.rename(columns={"Adlib reference (911a)": "911##a"})

    df_aleph = df_aleph.set_index(list(df_aleph)[1])
    df_aleph.index.names = ["סימול"]
    df_aleph = df_aleph.iloc[:, 0:1]
    df_aleph.columns = ["System number"]

    df = df.join(df_aleph, how="left")

    return df, df_aleph


def get_parent_root_mms_id(index):
    return ROOTID_finder(index)


def create_MARC_773(df):
    for index, row in df.iterrows():
        if "$$cFonds Record" in row["351"]:
            continue
        root_index, root_title = get_root_index_and_title(index,df)
        df.loc[index, "77318"] = f"$$t{root_title}$$w{root_index}"
    return df


def format_cat_date(df):
    """
        convert the date into YYMM format for construction of the 921/933 fields.
    :param df: the original dataframe
    :return: the modified dataframe with the reformatted cataloguing date field.
    """
    if column_exists(df, clean_text("תאריך הרישום")):
        cat_date_col = clean_text("תאריך הרישום")
    else:
        cat_date_col = process.extractOne("date_cataloguing", list(df.columns))

    df[cat_date_col] = df[cat_date_col].astype(str).apply(clean_date_format)

    df[cat_date_col] = df[cat_date_col].apply(
        lambda x: datetime.strftime(dateutil.parser.parse(x), "%Y%m")
        if len(x) > 6
        else x
    )

    return df


def create_date_format(string_date):
    logger = logging.getLogger(__name__)
    string_date_to_datetime = ""
    date_formats = [
        "%Y-%m-%d",
        "%Y-%m",
        "%Y-%m",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%Y%m%d %H:%M",
        "%d/%m/%Y %H:%M",
        "%m/%d/%y %H:%M",
        "%d-%m-%Y",
        "%d.%m.%Y",
    ]
    i = 0
    while True:
        try:
            string_date_to_datetime = datetime.strptime(string_date, date_formats[i])
            break
        except ValueError:
            i += 1
        except IndexError:
            logger.error(
                f"[921/933] didn't find the right date format for [{string_date}]"
            )
            sys.exit()
    return datetime.strftime(string_date_to_datetime, "%Y%m")


def construct_921(df):
    df["921"] = df["921"].map(Authority_instance.cataloger_name_mapper)

    try:
        df["921"] = (
            "$$a"
            + df["921"].map(str)
            + " "
            + df["תאריך הרישום"].apply(create_date_format)
        )
    except Exception as e:
        sys.stderr.write(f"[Error] please correct {e}")
        sys.exit()
    df["921"].apply(lambda x: "$$a" + x)
    return df


def construct_933(df):
    df_explode_933 = (
        df["933"]
        .str.split(";", expand=True)
        .rename(columns=lambda x: f"933_{x + 1}")
        .replace(np.nan, "")
        .replace("nan", "")
    )
    for col in list(df_explode_933):
        df_explode_933[col] = df_explode_933[col].map(
            Authority_instance.cataloger_name_mapper
        )
    df_explode_933 = df_explode_933.replace(np.nan, "")
    df = pd.concat([df, df_explode_933], axis=1)

    # TODO rewrite this loop - too messy
    for index, row in df.iterrows():
        for col in list(df_explode_933.columns):
            if row[col] == "" or row[col] == np.nan or row[col] == "nan":
                continue
            else:
                df.loc[index, col] = (
                    "$$a"
                    + str(row[col])
                    + " "
                    + create_date_format(df.loc[index, "תאריך הרישום"])
                )
    df = drop_col_if_exists(df, "933")
    return df


def create_MARC_921_933(df):
    """
        Fields [שם הרושם] [תאריך הרישום] are converted into the format of NLI cataloguer signature.
    The mapping is defined in the AuthorityFiles Class instance - in the cataloger_name_mapper attribute.
    The format of the MARC encoded 921/933 fields is "F[first name + last name initials] MMYYYY

    Change the catalogers names to their Aleph abbriviation as defined in above dictionary
    Add [year][month] to the string and place value in 921##a field.

    steps:
    ------
    1. extract extract the unique catalogers names from field [שם הרושם]
    2. match the names to the existing names of cataloguer in the controlled vocabulary
    3. map and replace the cataloguer names with the correct names of the controlled vocabulary
    4. map and replace the cataloguer names to their Aleph codes.
    5. construct the 921 field following the NLI guidelines - "$$a[cataloguer code] [YYYYMM]"

    NOTES for current version:
    --------------------------
    in the meantime its year month are static figures
    if cataloger field was empty a default value was introduced

    :param df:
    :return: The modified dataframe with the new 921 and 933 fields
    """
    # initialize 921/933 columns
    df["921"] = df["שם הרושם"]

    if df["921"].str.contains(";").any():
        df["933"] = ""
        multiple_cataloguer = df[df["921"].str.contains(";")].index.values
        for index in multiple_cataloguer:
            first_cataloguer = first_creator(df.loc[index, "921"]).strip()
            df.loc[index, "933"] = all_rest_creators(df.loc[index, "921"]).strip()
            df.loc[index, "921"] = first_cataloguer

    df = construct_921(df)

    if column_exists(df, "933"):
        df = construct_933(df)

    return df


def create_MARC_BAS(df):
    """

    :param df: the original Dataframe
    :return: The modified datafrmae with the additional BAS encoded field contaning 'VIS'
    """
    BAS = input("Please fill in the 906/BAS for this collection: \n")
    df["906"] = f"$$a{BAS.upper()}"
    return df


def create_MARC_948(df):
    """
        Adding OWN field to the data frame.
    :param df: the original Dataframe
    :return: The modified datafrmae with the additional OWN encoded field contaning 'NNL'
    """
    df["948"] = "$$aNNL"
    return df


def create_MARC_FMT(df):
    """
        Adding FMT field to the data frame.
        derived from Leader/006.
        for the sake of the example it is assumed that the collection contains mixed materials,
        and that Leader/006 = 'p'.

    sources:
    ---------
    LoC MARC21 Guidelines/Leader
    ExLibris/Logic for assigning FMT
    :param df: the original Dataframe
    :return: The modified datafrmae with the additional OWN encoded field contaning 'NNL'
    """
    df["FMT"] = "MX"
    return df


def create_MARC_999(df):
    """
        999##a - in the meantime not defined (it should be first hierarchy of 'סוג חומר')
        999##b - static value is 'NOULI'

    :param df:
    :return:
    """
    df["999_1"] = "$$bNOULI"
    df["999_2"] = "$$bNOOCLC"
    df["999_3"] = "$$aARCHIVE"

    return df


def create_citation_heb(index, collection_name):
    citation_heb = collection_name + ", " + "הספריה הלאומית, סימול: " + index
    return citation_heb


def create_citation_eng(index, collection_name):
    citation_eng = (
        collection_name + ", " + "National Library of Israel, Reference code: " + index
    )
    return citation_eng


def create_MARC_524(df, collection_id):
    """
        524$$a - Preferred Citation of Described Materials Note
    :param df:
    :return:
    """

    collection_name_heb = Authority_instance.df_credits.loc[collection_id, "שם הארכיון"]
    collection_name_eng = Authority_instance.df_credits.loc[
        collection_id, "שם הארכיון באנגלית"
    ]

    for index, row in df.iterrows():
        citation_heb = "$$a" + create_citation_heb(index, collection_name_heb)
        citation_eng = "$$a" + create_citation_eng(index, collection_name_eng)
        df.loc[index, "524_1"] = citation_heb
        df.loc[index, "524_2"] = citation_eng

    return df


def create_MARC_600(collection):
    """
    create column for personal entry names and add column to table as number
    of names occurances that appear in field

    colomn [מילות מפתח_אישים]
    Subject added entry in which the entry element is a personal name.
    First Indicator - Type of personal name entry element = 1 - Surname

    :param collection: the entire collection instance
    :return:

    """
    df = collection.df_final_data

    if column_exists(df, "מילות מפתח - אישים"):
        col = "מילות מפתח - אישים"
        df = aleph_creators(df, col, with_relators=False)
        df["6001"] = df[col]
        df = drop_col_if_exists(df, col)

        df = explode_col_to_new_df(df, "6001")
        df = df.fillna("")

        df = drop_col_if_exists(df, "6001")

    return df


def create_MARC_610(collection):
    """
    create column for personal entry names and add column to table as number
    of names occurances that appear in field
    :param df:
    :return:
    :param: df: the dataframe

    """
    df = collection.df_final_data
    if column_exists(df, "מילות מפתח - מוסדות"):
        col = "מילות מפתח - מוסדות"
    elif column_exists(df, "מילותמפתחמוסדות"):
        col = "מילותמפתחמוסדות"
    else:
        return df

    df = aleph_creators(df, col, with_relators=False)
    df["6102"] = df[col]
    df = drop_col_if_exists(df, col)

    df = explode_col_to_new_df(df, "6102")
    df = df.fillna("")

    df = drop_col_if_exists(df, "6102")

    return df


def create_MARC_630(collection):
    """
    create coloumn for first creator
    check if creator is a person of a corporate body - against the CVOC of creators roles,
    and insert the value in the respective colomn

    Column [מילות מפתח יצירות]
    Subject added entry in which the entry element is a uniform title.

    first indicator: Nonfiling - empty
    second indicator: Thesaurus

    4 - Source not specified
    Subfield used: $a


    :param: df: the dataframe

    """

    ad = alphabet_detector.AlphabetDetector()
    df = collection.df_final_data

    if column_exists(df, "מילות מפתח_יצירות"):
        col = "מילות מפתח_יצירות"
        df = aleph_creators(df, col, mode="WORKS", branch=collection.branch)
        df["63004"] = df[col]
        df = drop_col_if_exists(df, "מילות מפתח_יצירות")

        df = explode_col_to_new_df(df, "63004")
        df = df.fillna("")

        df = drop_col_if_exists(df, "63004")

        collection.df_final_data = df

    return collection


def update_008_no_linguistic_content(val_008):
    return "".join((val_008[:35], "zxx", val_008[38:]))


def create_MARC_336(df):
    """
        he form of communication through which a work is expressed.
         Used in conjunction with Leader /06 (Type of record), which indicates the general type of content
         of the resource.
         Field 336 information enables expression of more specific content types and content types from various lists.

         subfields
            $a - Content type term (R) - Content type of the work being described.
            $b - Content type code (R) - Code representing the content type of the work being described.
            $2 - Source (NR) - MARC code that identifies the source of the term or code used to record the content
            type information. Code from: Genre/Form Code and Term Source Codes.
            The Project mapped all it's Archival Materiel terms from its Archival Materiel controlled vocabulary
            to the RDA content type terms and constructed the subfields according to the agreed upon mapping with Ahava.
            (Archival Material - RDA content type mapping table)
    :param df:
    :return:
    """
    arch_mat_map_336 = (
        Authority_instance.df_arch_mat_auth.loc[
            :, ["ARCHIVAL_MATERIAL", "rdacontent 336"]
        ]
        .set_index("ARCHIVAL_MATERIAL")
        .to_dict()["rdacontent 336"]
    )

    for index, row in df.iterrows():
        lst_336 = row["סוג חומר"].split(";")
        lst_336 = list(map(str.strip, lst_336))
        lst_336 = replace_lst_dict(lst_336, arch_mat_map_336)
        if len(lst_336) == 1 and lst_336[0] == "$$astill image$$bsti$$2rdacontent":
            df.loc[index, "008"] = update_008_no_linguistic_content(row["008"])
        df.loc[index, "336"] = ";".join(lst_336)

    df = remove_duplicate_in_column(df, "336")

    df_explode_336 = (
        df["336"].str.split(";", expand=True).rename(columns=lambda x: f"336_{x + 1}")
    )
    df_explode_336 = df_explode_336.replace(np.nan, "")
    df = pd.concat([df, df_explode_336], axis=1)
    cols_336 = [x for x in list(df.columns.values) if "336" in x]
    df = drop_col_if_exists(df, "336")
    df = drop_col_if_exists(df, "סוג חומר")

    return df, df_explode_336


def create_MARC_337_338(df):
    """
        ccording to Ahava, the RDA Media Type and RDA Carrier Type should be for all the resources in the project as follows:

        337 = online resource
        338 = computer
        Therefore the two fields will contain a constant string:

        '337' = 𝑎𝑜𝑛𝑙𝑖𝑛𝑒𝑟𝑒𝑠𝑜𝑢𝑟𝑐𝑒bcr$$2rdacarrier
        '338' = 𝑎𝑐𝑜𝑚𝑝𝑢𝑡𝑒𝑟bc$$2rdamedia

    :param df:
    :return:
    """
    df["337"] = "$$acomputer$$bc$$2rdamedia"
    df["338"] = "$$aonline resource$$bcr$$2rdacarrier"

    return df


def last_index_of_reoccurring_column(df: pd.DataFrame, col_name: str) -> int:
    """

    @rtype: int
    """
    return len([col for col in list(df.columns) if col_name in col])


# TODO - refactor messy messy function: use the Authority_instance
def create_MARC_534(df):
    """
        From LoC MARC21 format for bibliogrphic records guidelines: Descriptive data for an original item when
         the main portion of the bibliographic record describes a reproduction of that item and the data differ.
         Details relevant to the original are given in field 534.

         The resource being cataloged may either be a reproduction (e.g., scanned image, or PDF), or an edition
         that is similar enough that it could serve as a surrogate for the original (e.g., HTML).

            $e - Physical description, etc. of original (NR)
            $p - constant text: "מנשא והפורמט הפיזי של הפריט המקורי" + "carrier and format of original item"
            $c - Publication, distribution, etc. of original (NR)

    :param df:
    :return:
    """

    # create a dataframe as source for mapping media/format values
    df_media_format_mapping = Authority_instance.df_media_format_auth.loc[
        Authority_instance.df_media_format_auth.index, ["MEDIA_FORMAT", "MARC21 534"]
    ]

    # export media/format mapping DF to dictionary
    arch_media_format_map_534 = pd.Series(
        df_media_format_mapping["MARC21 534"].values,
        index=df_media_format_mapping.MEDIA_FORMAT.values,
    ).to_dict()

    df = remove_duplicate_in_column(df, "מדיה + פורמט")

    for index, row in df.iterrows():
        try:
            lst_534 = row["מדיה + פורמט"].split(";")
        except:
            print(df.columns)
        lst_534_final = [
            "$$pמנשא והפורמט הפיזי של הפריט המקורי." + "$$e" + s.strip()
            if s != ""
            else ""
            for s in lst_534
        ]
        lst_534_final = replace_lst_dict(lst_534_final, arch_media_format_map_534)
        df.loc[index, "534"] = ";".join(lst_534_final)

    df = remove_duplicate_in_column(df, "534")
    df = explode_col_to_new_df(df, "534")
    df = drop_col_if_exists(df, "534")

    if column_exists(df, "תאריך יצירת החפץ / הטקסט המקורי מוקדם"):
        new_534_col = "534_" + str(last_index_of_reoccurring_column(df, "534"))
        df[new_534_col] = (
            df["תאריך יצירת החפץ / הטקסט המקורי מוקדם"].map(str)
            + " - "
            + df["תאריך יצירת החפץ / הטקסט המקורי מאוחר"].astype(str)
        )
        df[new_534_col] = df[new_534_col].map(
            lambda x: "$$aנוצר לראשונה בין התאריכים: " + str(x).replace(".0", "")
            if x != " - "
            else ""
        )

    return df


def create_MARC_STA(df):
    df["STA"] = "$$aSUPPRESSED"
    return df


def create_MARC_590_digitization_data(row):
    new_value = "מסלולי דיגיטציה: "
    if str(row["מסלול דיגיטציה"]) != "":
        new_value = new_value + str(row["מסלול דיגיטציה"]) + ";"

    if str(row["סריקה דו-צדדית"]) != "":
        new_value = new_value + "סריקה דו-צדדית: " + str(row["סריקה דו-צדדית"]) + ";"
    if str(row["מספר קבצים מוערך"]) != "":
        new_value = (
            new_value + "מספר קבצים מוערך: " + str(row["מספר קבצים מוערך"]) + ";"
        )
    if new_value == "מסלולי דיגיטציה: ":
        return ""

    value = "$$a" + new_value
    return value.strip()


# TODO refactor this messy messy function with correct column names
def create_MARC_590(df, copyright_analysis_done):
    df["590_1"] = df.apply(lambda row: create_MARC_590_digitization_data(row), axis=1)

    # TODO - לא כל הרשומות רלוונטיות לזכויות יוצרים - האם לסמן על כל הרשומות כמוכנות לניתוח זכויות יוצרים?

    if copyright_analysis_done:
        for index, row in df.iterrows():
            if "לא מוכן" in str(row["הערות לא גלוי למשתמש"]):
                df.loc[index, "הערות לא גלוי למשתמש"] = (
                    str(row["הערות לא גלוי למשתמש"]).strip("לא מוכן לניתוח זכויות יוצרים")
                    + ";Visual art ready for copyright analysis"
                )
            else:
                continue

    if not copyright_analysis_done:
        df["הערות לא גלוי למשתמש"] = (
            df["הערות לא גלוי למשתמש"].astype(str)
            + ";Visual art ready for copyright analysis"
        )

    df = explode_col_to_new_df(df, "הערות לא גלוי למשתמש", start=2)
    cols_590 = [col for col in list(df.columns) if "הערות לא גלוי למשתמש" in col]
    for col in cols_590:
        new_col = "590" + col[col.find("_") :]
        df[new_col] = df[col].apply(lambda x: "$$a" + str(x) if str(x) != "" else "")

    return df


def create_MARC_584(df):
    # TODO not used in preprocessing2
    if (
        column_exists(df, "ACCURALS")
        or column_exists(df, "אוסףפתוח")
        or column_exists(df, "אוסף פתוח")
    ):
        accurals_mapper = {
            "כן": "האוסף המקורי ממשיך לצבור חומרים (אוסף פתוח)a$",
            "לא": "$aהאוסף המקורי אינו צובר חומרים חדשים (אוסף סגור)",
        }
        df["584"] = df["584"].map(accurals_mapper)
        df["584"] = df["584"].apply(lambda x: "$a" + x)
        return df


def create_907_dict(ROS_file: minidom) -> dict:
    """
        The function takes the MARCxml file of the collection, which resides in ./[branch]/[collection]/Digitization/ROS
        directory, and that was parsed into a minidom xml object, extract the MMS ID (001 tag) and the 907 (Rosetta
        link) field, with all it's subfields. Saves the MMS ID and 907 subfield in a dictionary of dictionaries.
    :param ROS_file: The MARCxml file of the collection parsed into a minidom object.
    :return: dictionary of dictionaries, which key is the MMS ID and the inner dictionary is the extracted 907 field
    """
    d = {}
    for record in ROS_file.getElementsByTagName("record"):
        # for e in record.getElementsByTagName('controlfield'):
        #     if e.attributes['tag'].value == '001':
        #         id = e.childNodes[0].data
        id = next(
            e.childNodes[0].data
            for e in record.getElementsByTagName("controlfield")
            if e.attributes["tag"].value == "001"
        )

        dd = {}
        for e in record.getElementsByTagName("datafield"):
            if e.attributes["tag"].value == "907":
                for sb in e.getElementsByTagName("subfield"):
                    dd["907" + sb.attributes["code"].value] = sb.childNodes[0].data

        d[id] = dd
    return d


def create_907_value(dict_907):
    words = []
    for tag, value in dict_907.items():

        if len(value) == 0:
            return ""
        else:
            words.append(tag[3:] + value)
    return_val = "$$" + "$$".join(words)
    return_val.replace("$$$$", "$$")
    return return_val


def add_MARC_907(collection):
    rosetta_file_path = lookup_rosetta_file(
        collection.digitization_path, collection.collection_id
    )
    rosetta_file = minidom.parse(rosetta_file_path)
    df = collection.df_final_data
    rosetta_dict = create_907_dict(rosetta_file)

    df["907"] = ""
    for index, row in df.iterrows():
        try:
            if index == np.nan:
                sys.stderr.write(f"this index: {index} for {row['סימול']} is missing")
            elif str(index) not in rosetta_dict.keys():
                sys.stderr.write(
                    f"there is no 907 field for : {index}, for call number {row['סימול']}\n."
                )
                sys.exit()
            elif len(rosetta_dict[str(index)]) == 0:
                continue
            else:
                df.loc[index, "907"] = create_907_value(rosetta_dict[str(index)])
        except:
            pass

    collection.df_final_data = df

    return collection


def create_035_dict(file):
    d = {}
    for record in file.getElementsByTagName("record"):
        mms_id = next(
            e.childNodes[0].data
            for e in record.getElementsByTagName("controlfield")
            if e.attributes["tag"].value == "001"
        )
        dd = {}
        for e in record.getElementsByTagName("datafield"):
            if e.attributes["tag"].value == "035":
                for sb in e.getElementsByTagName("subfield"):
                    dd["035"] = (
                        "$$" + sb.attributes["code"].value + sb.childNodes[0].data
                    )
                    dd["035"] = dd["035"].replace("$$$$", "$$")
        d[mms_id] = dd
    return d


def add_MARC_035(collection):
    rosetta_file_path = lookup_rosetta_file(
        collection.digitization_path, collection.collection_id
    )
    rosetta_file = minidom.parse(rosetta_file_path)
    dict_035 = create_035_dict(rosetta_file)

    df = collection.df_final_data

    df["035"] = ""
    for mms_id, row in df.iterrows():
        try:
            if mms_id == np.nan:
                sys.stderr.write(f"this index: {mms_id} for {row['סימול']} is missing")
            elif str(mms_id) not in dict_035.keys():
                sys.stderr.write(
                    f"there is no 035 field for : {mms_id}, for call number {row['סימול']}\n."
                )
                sys.exit()
            elif len(dict_035[str(mms_id)]) == 0:
                continue
            else:
                df.loc[mms_id, "035"] = create_907_value(dict_035[str(mms_id)])
        except:
            pass
    collection.df_final_data = df

    return collection


def create_field_dict(file, tag):
    d = {}
    for record in file.getElementsByTagName("record"):
        mms_id = next(
            e.childNodes[0].data
            for e in record.getElementsByTagName("controlfield")
            if e.attributes["tag"].value == "001"
        )
        dd = {}
        for e in record.getElementsByTagName("datafield"):
            if e.attributes["tag"].value == tag:
                val = list()
                for sb in e.getElementsByTagName("subfield"):
                    # dd["952"] = (
                    #         "$$" + sb.attributes["code"].value + sb.childNodes[0].data
                    # )

                    val.append(
                        "$$" + sb.attributes["code"].value + sb.childNodes[0].data
                    )
                    # if val_952.startswith("$$$$"):
                    #     val_952 = dd["952"][2:]
                dd[tag] = "".join(val)

        d[mms_id] = dd
    return d


def update_df_with_field_dict(df, tag_dict, tag):
    for mms_id, row in df.iterrows():
        try:
            if mms_id == np.nan:
                sys.stderr.write(f"this index: {mms_id} for {row['סימול']} is missing")
            elif str(mms_id) not in tag_dict.keys():
                sys.stderr.write(
                    f"there is no 952 field for : {mms_id}, for call number {row['סימול']}\n."
                )
                sys.exit()
            elif len(tag_dict[str(mms_id)]) == 0:
                continue
            else:
                df.loc[mms_id, tag] = create_907_value(tag_dict[str(mms_id)])
        except Exception as e:
            sys.stderr.write(e)
            pass
    if tag == "952":
        df[tag] = df[tag].astype("str") + df["952_g"]
        df = drop_col_if_exists(df, "952_g")
    return df


def add_copyright_field_from_alma(collection):
    rosetta_file_path = lookup_rosetta_file(
        collection.digitization_path, collection.collection_id
    )
    rosetta_file = minidom.parse(rosetta_file_path)

    dict_952 = create_field_dict(rosetta_file, "952")
    collection.df_final_data = update_df_with_field_dict(
        collection.df_final_data, dict_952, tag="952"
    )

    dict_915 = create_field_dict(rosetta_file, "915")
    collection.df_final_data = update_df_with_field_dict(
        collection.df_final_data, dict_915, tag="915"
    )

    dict_903 = create_field_dict(rosetta_file, "903")
    collection.df_final_data = update_df_with_field_dict(
        collection.df_final_data, dict_903, tag="903"
    )

    dict_939 = create_field_dict(rosetta_file, "939")
    collection.df_final_data = update_df_with_field_dict(
        collection.df_final_data, dict_939, tag="939"
    )

    return collection


def add_MARC_597(df):

    df["597"] = "$$a" + df["קרדיט עברית"].astype(str) + "$$a" + df["קרדיט אנגלית"].astype(str)
    df = drop_col_if_exists(df, "קרדיט עברית")
    df = drop_col_if_exists(df, "קרדיט אנגלית")

    return df


def export_MARCXML_final_table(collection):
    logger = logging.getLogger()
    logger.info(f"[MARCXML] create final MARC XML file for {collection.collection_id}")
    df_final_cols = sorted([
        x for x in list(collection.df_final_data.columns) if x[0].isdigit()
    ] + ["LDR"])
    collection.marc_data = collection.df_final_data[df_final_cols]

    counter, run_time = collection.create_MARC_XML()
    sys.stderr.write(
        f"{counter} total records written to file in {run_time} seconds.\n\n"
    )

    return collection


def create_MARC_255(df):
    if "קנה מידה" in list(df.columns):
        df["255"] = df["קנה מידה"].map(
            lambda x: "$$aקנה מידה [" + str(x) + "]" if str(x).strip() != "" else ""
        )
        return df


def create_MARC_650_branch(collection):
    """
        Create subject for the Project 4 branches: Architect, Dance, Design, Theater in MARC field 650
        according to the branch  attribute of the collection.
    :param collection: The collection object
    :return: the collection object with the df_final_data, with the added 650 MARC field
    """
    if collection.branch == "VC-Architect":
        # last_index_of_reoccurring_column(collection.df_final_data, "650")
        collection.df_final_data["650 7"] = "$$aArchitecture$$zIsrael$$9lat$$2NLI"
    elif collection.branch == "VC-Dance":
        collection.df_final_data["650 7"] = "$$aDance$$zIsrael$$9lat$$2NLI"
    elif collection.branch == "VC-Design":
        collection.df_final_data["650 7"] = "$$aDesign$$zIsrael$$9lat$$2NLI"
    elif collection.branch == "VC-Theater":
        collection.df_final_data["650 7"] = "$$aTheater$$zIsrael$$9lat$$2NLI"

    return collection


def create_default_040(df):
    df["040"] = "$$bheb$$erda"
    return df


def create_MARC_590_sponsors(df, branch):
    """
        Add 4 fields of MARC 590 for sponsors
    @param df: The
    @param branch: The branch - one of 4: Architect, Dance, Design, Theater.
    """

    col_number = last_index_of_reoccurring_column(df, "590")
    df[f"590_{str(col_number + 1)}"] = "$$asponsor: Jerusalem and Heritage"
    df[f"590_{str(col_number + 2)}"] = "$$asponsor: Landmarks"
    df[
        f"590_{str(col_number + 3)}"
    ] = "$$asponsor: The Judaica collection at the Harvard University Library"

    if branch == "VC-Architect":
        df[
            f"590_{str(col_number + 4)}"
        ] = "$$asponsor: Bezalel Academy of Arts and Design, Jerusalem"

    if branch == "VC-Dance":
        df[f"590_{str(col_number + 4)}"] = "$$asponsor: Batsheva Dance Company"

    if branch == "VC-Design":
        df[
            f"590_{str(col_number + 4)}"
        ] = "$$asponsor: Shenkar - Engineering. Design. Art."

    if branch == "VC-Theater":
        df[f"590_{str(col_number + 4)}"] = "$$sponsor: University of Haifa"
    return df
