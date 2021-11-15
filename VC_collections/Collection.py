import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from shutil import copyfile

import gspread
import numpy as np
import pandas as pd
from alphabet_detector import AlphabetDetector
from oauth2client.service_account import ServiceAccountCredentials
from pymarc import XMLWriter, Record, Field

import preprocess_0
import Data.vis_907_dict
from Data import vis_907_dict
from VC_collections.project import get_branch_colletionID
from . import columns
from .AuthorityFiles import Authority_instance
from .fieldmapper import (
    catalog_field_mapper,
    collection_field_mapper,
    final_fields_back_mapper,
)
from .files import (
    create_directory,
    write_excel,
    find_newest_file_in_list,
)


def retrieve_collection():
    CMS, branch, collection_id = get_branch_colletionID()
    return Collection(CMS, branch, collection_id)


def get_google_drive_credentials():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]

    clientsecret_file_path = Path("google_drive_api") / "client_secret.json"
    if clientsecret_file_path.is_file():
        try:
            return ServiceAccountCredentials.from_json_keyfile_name(
                clientsecret_file_path, scope
            )
        except OSError as e:
            sys.stderr.write("problem with creds!")
            return None
    else:
        sys.stderr.write("thre is no [client_secret.json] file!")
        return None


def connect_to_google_drive():
    creds = get_google_drive_credentials()
    client = gspread.authorize(creds)

    return client


def choose_catalog_gspread(client, collection_id):
    files = [
        file
        for file in client.list_spreadsheet_files()
        if collection_id.lower() in file["name"].lower()
    ]
    if len(files) == 0:
        sys.stderr.write(f"no file for {collection_id} found in google drive \n")
        return client, input("if you have the ID of the file, please enter manually: ")

    for index, file in enumerate(files):
        print(index, ":", file["name"])
    while True:
        try:
            file_index = int(
                input(
                    "which file of the following do you choose? type the index number, "
                    "if none of the above return -1: \n"
                )
            )
            if file_index == str(-1):
                return (
                    client,
                    input("if you have the ID of the file, please enter manually: "),
                )
        except ValueError:
            print("Please re-enter the index number of the file you want to parse: ")
            continue
        else:
            break

    return client, files[int(file_index)]["id"], files[int(file_index)]["name"]


# TODO change this function to take only the relevant sheets and not all of them
def create_xl_from_gspread(client: gspread.client.Client, file_id: str) -> dict:
    """
    the function opens the Google Sheet spreadsheet and  iterates over all the sheets within the file, creates a
    dataframe from each sheet and adds it to a dictionary of dataframes.

    :param client: the google drive/sheets client api created by the creadentials in the
        'google_drive_api/client_secret.json' file.
    :param file_id: the file id of the google spreadsheet to parse
    :return: a dictionary dataframes - each dataframe represents a sheet within the google spreadsheet.
    """
    spreadsheet = client.open_by_key(file_id)
    all_sheets_as_dfs = {}
    worksheet_list = spreadsheet.worksheets()

    for sheet in worksheet_list:
        print(sheet)
        try:
            if sheet.row_values(2) is None:
                continue
        except Exception as e:
            sys.stderr.write(f"exception {e} in sheet: {sheet.title}")

        dict_ds = sheet.get_all_records(head=1)
        df = pd.DataFrame(dict_ds)
        all_sheets_as_dfs[sheet.title] = df

    return all_sheets_as_dfs


def export_entire_catalog(collection, df_sheets_dict, stage):
    if stage == "PRE_FINAL":
        file_path = collection.data_path_raw / (
            collection.collection_id + "_PRE_FINAL.xlsx"
        )
    elif stage == "FINAL":
        file_path = collection.data_path_processed / (
            collection.collection_id
            + "_final_"
            + datetime.now().strftime("%Y%m%d")
            + ".xlsx"
        )
    elif stage == "PRE1_FINAL":
        file_path = collection.data_path_raw / (
            collection.collection_id + "_PRE1_FINAL.xlsx"
        )

    if type(df_sheets_dict) == dict:
        write_excel(df_sheets_dict, file_path, list(df_sheets_dict.keys()))

    # TODO check if collectin.full_catalog is of type df or of type dict of dfs?
    else:
        pass
        write_excel(df_sheets_dict, file_path, "Sheet1")


def remove_unnamed_cols(df):
    logger = logging.getLogger(__name__)
    columns = [col for col in list(df.columns) if "unnamed" not in col.lower()]
    unnamed_columns = [x for x in list(df.columns) if x not in columns]
    logger.info(f"Removing  unnamed columns. Found {len(unnamed_columns)} ")
    return df[columns]


def remove_trailing_zero(df):
    ad = AlphabetDetector()
    if ad.is_hebrew(df.columns[0]):
        cols = [col for col in list(df.columns) if "תאריך" in col] + [
            "מספר מיכל",
            "מספר קבצים מוערך",
            "ברקוד",
        ]
    else:
        cols = [col for col in list(df.columns) if "תאריך" in col] + [
            "CONTAINER",
            "EST_FILES_NUM",
            "BARCODE",
        ]
    for col in cols:
        if col not in list(df.columns):
            df[col] = ""
    df[cols] = df[cols].replace(r"\.0$", "", regex=True)

    return df


def remove_empty_rows(df):
    df = df.replace("", np.nan)
    df = df.dropna(how="all")
    if "סימול פרויקט" in list(df.columns):
        df = df.dropna(subset=["סימול פרויקט"])

    if df.index.name is not None:
        print(df.index.name)
        index = df.index.name
        df_a = df.reset_index(drop=True)
        df = df_a.reset_index(drop=True).dropna().set_index(index)
    return df


def remove_instructions_row(df):
    if (
        df.iloc[0].str.contains("שדה חובה!!").any()
        or df.iloc[0].str.contains("שדה חובה").any()
    ):
        # remove instruction line
        return df.loc[
            1:,
        ]
    else:
        return df


def fill_missing_cataloging_date(df):
    logger = logging.getLogger(__name__)
    if "DATE_CATALOGING" in list(df.columns):
        col = "DATE_CATALOGING"
    elif "תאריך הרישום" in list(df.columns):
        col = "תאריך הרישום"
    else:
        sys.stderr.write(
            "There is no column for cataloging date in table, please check!"
        )
        sys.exit()
    print(f"max date in {col} is: {pd.to_datetime(df[col], errors='coerce').max()}")
    latest_date = pd.to_datetime(df[col], errors="coerce").max()

    df = df.replace(r"^\s*$", np.nan, regex=True)

    if df[col].isna().sum() > 0:
        logger.info(
            f"[CATALOGING_DATE] Filling missing cataloging date values with calculated max cataloging date"
        )
        df[col].fillna(latest_date, inplace=True)
    return df.replace(np.nan, "")


def clean_catalog(df):
    """
    initial cleanup of the row catalog:
    - remove instruction/guideline row (mostly row 2)
    - fill n/a values with empty string
    - removing columns which were added because of indexing and unindexing with 'unnamed' in heading
    - fill missing dates - cataloging date
    :param df: The dataframe to cleanup
    :return: the cleanup dataframe

    """
    df = df.rename(columns={"סימול/מספר מזהה": "סימול", "סימול פרויקט": "סימול"})
    df = df.fillna("")
    df = remove_instructions_row(df)
    df = remove_unnamed_cols(df)
    df = remove_trailing_zero(df)
    df = fill_missing_cataloging_date(df)
    for col in list(df.columns):
        df = columns.clean_text_cols(df, col)
        df = columns.rstrip_semicolon(df, col)
        df = columns.strip_whitespace_af_semicolon(df, col)

    return df


def strip_column_name(cols_names):
    new_columns_names = []
    for col in cols_names:
        col = "".join(e.strip().lower() for e in str(col) if e.isalnum())
        new_columns_names.append(col)
    return new_columns_names


def map_field_names_to_english(col_names: list, mapper: dict) -> list:
    # replace the field name according to the generic field mapper
    print("col_names:", col_names)
    try:
        new_col_names = [mapper.get(x, "no mapping").upper() for x in col_names]
        if "NO MAPPING" in new_col_names:
            print(
                "columns not mapped:",
                "\n".join([f"{x}: {mapper.get(x)}" for x in col_names]),
            )
    except:
        for col in col_names:
            sys.stderr.write(f"{col} - does not exist")
        sys.exit()
    return new_col_names


def add_current_owner(df_collection, df_credits, collection_id):

    if "סימול האוסף" in list(df_collection.columns):
        df_collection = df_collection.set_index("סימול האוסף")
    elif "סימול הארכיון" in list(df_collection.columns):
        df_collection = df_collection.set_index("סימול הארכיון")

    desc_level_value = df_collection.loc[collection_id, "רמת תיאור"]

    if (
        "בעלים נוכחי" in list(df_collection.columns)
        and df_collection.loc[
            df_collection["רמת תיאור"] == desc_level_value, "בעלים נוכחי"
        ][0]
        != ""
    ):
        if df_credits.loc[collection_id, "מיקום הפקדה עבור בעלים נוכחי"] != "":
            df_collection.loc[collection_id, "בעלים נוכחי"] = df_credits.loc[
                collection_id, "מיקום הפקדה עבור בעלים נוכחי"
            ]
            return df_collection

    return df_collection.reset_index()


def find_last_updated_gspread(client, collection_id):
    files = [
        file
        for file in client.list_spreadsheet_files()
        if collection_id.lower() in file["name"].lower()
    ]
    if len(files) == 0:
        sys.stderr.write(f"no file for {collection_id} found in google drive \n")
        return client, input("if you have the ID of the file, please enter manually: ")

    file_index = find_newest_file_in_list(
        files, collection_id + "_final_to_alma_", mode="mid"
    )

    return (
        client,
        files[int(file_index)]["id"],
        files[int(file_index)]["name"],
    )


def add_missing_cols_to_table(df):
    logger = logging.getLogger(__name__)
    missing_cols = list(set(final_fields_back_mapper.keys()) - set(list(df.columns)))
    for col in missing_cols:
        df[col] = ""
    logger.info(f"add missing column to table: {col}")

    return df


def file_907_exists(aleph_custom04_path, collection_id):
    sys_num_file = Path(aleph_custom04_path) / f"{collection_id}_alma_sysno.xlsx"
    # input(sys_num_file)
    if sys_num_file.is_file():
        return True
    else:
        return False


class Collection:
    _project_branches = ["Architect", "Dance", "Design", "Theater"]
    _catalog_sheets = {
        "df_catalog": "קטלוג",
        "df_collection": "אוסף",
        "df_personalities": "אישים",
        "df_corporation": "מוסדות",
    }

    @classmethod
    def branches(cls):
        return cls._project_branches

    @staticmethod
    def get_sheet(xl, sheet):
        """

        :param xl:
        :param sheet:
        :return:
        """
        assert sheet.strip() in xl.sheet_names, f"sheet {sheet} does not exist in file."
        return xl.parse(sheet.strip())

    def make_catalog_copy(self):
        """
            creates a safe copy of the original xlsx file ("PRE_FINAL.xlsx"), and adds 'safe_copy' suffix to the file.
        :return:  path to new file, or none if it already exists.
        """
        print("self.data_path_raw:", self.data_path_raw)
        for file in os.listdir(self.data_path_raw):
            filename = os.fsdecode(file)

            if "PRE_FINAL" in filename:  # this tests for substrings
                file_path = os.path.join(self.data_path_raw, filename)
                print("NEW COPY", filename)
                new_file = file_path.replace(".xlsx", "_save_copy.xlsx")
                try:
                    copyfile(file_path, new_file)
                except shutil.SameFileError:
                    pass
                return new_file
            else:
                return None

    @staticmethod
    def replace_table_column_names(df, type):
        """
            The function replaces the column header names according to the field mapper dictionary.
            This is in order to create a unified table with the same structure and columnnames.
        :param type: type defines the table, whether it is the collection table or the catalog table.
        :param df: original dataframe
        :return: the modified dataframe with the new column headers
        """
        if type == "catalog":
            mapper = catalog_field_mapper
        elif type == "collection":
            mapper = collection_field_mapper

        logger = logging.getLogger(__name__)
        logger.info(
            "[HEADERS] strip column names from special characters and whitespaces."
        )
        df.columns = strip_column_name(list(df.columns))
        logger.info(
            f"[HEADERS] Changing Hebrew column names into English - according to field_mapper."
        )
        df.columns = map_field_names_to_english(list(df.columns), mapper)
        df = remove_unnamed_cols(df)
        return df

    def make_one_table(self):
        """
            creates one table by merging the catalog and the collection tables.
        :param self: the entire collection object
        :return: the collection object with the new attri
        """
        # turn index to string
        self.df_collection.index = self.df_collection.index.map(str)

        df_catalog = self.replace_table_column_names(
            remove_unnamed_cols(self.df_catalog), type="catalog"
        )

        df_collection = self.replace_table_column_names(
            remove_unnamed_cols(self.df_collection), type="collection"
        )
        df_collection = columns.drop_col_if_exists(df_collection, "מספרמערכתבאלף")

        print(
            "df_catalog columns:",
            "\n".join([f"{i}: {x}" for i, x in enumerate(list(df_catalog.columns))]),
        )
        print(
            "df_collection columns:",
            "\n".join([f"{i}: {x}" for i, x in enumerate(list(df_collection.columns))]),
        )
        if "UNITID" not in list(df_collection.columns):
            df_collection.index.name = "UNITID"
            # df_collection = df_collection.reset_index()
        else:
            df_collection.set_index("UNITID", inplace=True)

        if "UNITID" not in list(df_catalog.columns):
            df_catalog.index.name = "UNITID"
        else:
            df_catalog.set_index("UNITID", inplace=True)

        try:
            combined_catalog = pd.concat([df_collection, df_catalog], axis=0, sort=True)

        except:
            # df_collection.reset_index(inplace=True, drop=True)
            # df_catalog.reset_index(inplace=True, drop=True)
            combined_catalog = pd.concat([df_collection, df_catalog], axis=0, sort=True)

            sys.stderr.write(
                "the Collection and Catalog dataframes could not be combined"
            )
            sys.exit()

            combined_catalog = remove_unnamed_cols(combined_catalog)
            combined_catalog = combined_catalog.set_index("UNITID")
            combined_catalog.index = combined_catalog.index.map(str)

        return combined_catalog

    @classmethod
    def catalog_sheets(cls):
        """

        :return:
        """
        return cls._catalog_sheets

    def create_catalog_metadata_file(self):
        """
        creates a conf file in json format, with the metadata about the data used in the process:
        cms, branch, collection_id, BASE_PATH of the directory, google sheet file name of the used collection sheet,
        and the google sheet file id of the used collection google spreadsheet.
        """
        catalog_metadata_fields = [
            "cms",
            "branch",
            "collection_id",
            "BASE_PATH",
            "google_sheet_file_name",
            "google_sheet_file_id",
        ]

        catalog_metadata_dict = {
            key: str(self.__dict__[key]) for key in catalog_metadata_fields
        }

        file_path = self.data_path_reports / (self.collection_id + "_metadata.conf")

        with open(file_path, encoding="utf8", mode="w") as f:
            f.write(json.dumps(catalog_metadata_dict, indent=4))

    def fetch_data(self) -> dict:
        """
        :rtype: object

        """
        catalog_dfs = {}

        def remove_empty_rows(df):
            """

            :param df:
            :return:
            """
            df = df.replace("", np.nan)
            df = df.dropna(axis=0, how="all")
            if "סימול פרויקט" in list(df.columns):
                df = df.dropna(subset=["סימול פרויקט"])

            if df.index.name is not None:
                print(df.index.name)
                index = df.index.name
                df_a = df.reset_index(drop=True)
                df = df_a.reset_index(drop=True).dropna().set_index(index)
            return df

        def remove_instructions_row(df):
            """

            :param df:
            :return:
            """
            if (
                df.iloc[0].str.contains("שדה חובה!!").any()
                or df.iloc[0].str.contains("שדה חובה").any()
            ):
                # remove instruction line
                return df.loc[
                    1:,
                ]
            else:
                return df

        def get_works_sheet(xl_file, branch):
            """

            :param xl_file:
            :param branch:
            :return:
            """
            works_sheets = [x for x in xl_file.sheet_names if "יצירות" in x]
            if "יצירות" in works_sheets and len(works_sheets) == 1:
                return "יצירות"
            if "Dance" in branch:
                assert (
                    "יצירות - מחול" in xl_file.sheet_names
                ), " sheet יצירות - מחול does not exist in file."
                return "יצירות - מחול"
            elif "Architect" in branch:
                if "יצירות - אדריכלות" in xl_file.sheet_names:
                    return "יצירות - אדריכלות"
            elif "Theater" in branch:
                assert (
                    "יצירות - תאטרון" in xl_file.sheet_names
                ), " sheet יצירות - תאטרון does not exist in file."
                return "יצירות - תאטרון"

            else:
                return ""

        copy = self.make_catalog_copy()
        if copy is None:
            pass
        else:

            print(copy)
            try:
                xl = pd.ExcelFile(copy)
            except ValueError:
                sys.stderr.write("Invalid file path! check if collection exists.")

            for table, sheet in Collection.catalog_sheets().items():
                original_sheet = self.get_sheet(xl, sheet)
                catalog_dfs[sheet] = remove_empty_rows(
                    remove_instructions_row(original_sheet)
                )
            # add WORKS sheet to dfs if there is one
            try:
                catalog_dfs["יצירות"] = remove_empty_rows(
                    xl.parse(get_works_sheet(xl, self.branch))
                )
            except:
                pass
        try:
            catalog_dfs["קטלוג סופי"] = remove_empty_rows(xl.parse("קטלוג סופי"))

        except:
            pass

        return catalog_dfs

    def temp_preprocess_file(self, stage="PRE"):
        """
            exports the entire catalog sheets to a xlsx file, with the suffix '_preprocessing_test.xlsx'
        :param stage: in which stage of the process the data is exported: PRE, POST (default = "PRE").
        """
        if stage == "PRE":
            dataframe2export = self.full_catalog
        elif stage == "POST":
            dataframe2export = self.df_final_data
        dataframe2export.index = dataframe2export.index.astype(str)
        dt_now_temp = datetime.now().strftime("%Y%m%d")
        preprocess_filename = self.data_path_raw / (
            self.collection_id + "_" + dt_now_temp + "_preprocessing_test.xlsx"
        )
        write_excel(dataframe2export, preprocess_filename, "Catalog")

    def __init__(self, CMS: str, branch: str, collection_id: str, manual=True):
        """
             Initializer / Instance Attributes
        :param CMS: To which CMS the collection is intended to be imported to
        :param branch: The name of the project branch the collection belongs to (Architect , Design, Dance, Theater)
        :param collection_id: The collection identifier (call number)
        """

        self.cms = CMS
        self.branch = branch
        self.collection_id = collection_id
        self.dt_now = datetime.now().strftime("%Y%m%d")

        # create directory and sub-folders for collection
        self.BASE_PATH = Path.cwd().parent.absolute() / (branch) / collection_id

        # initialize directory with all folder and sub-folders for the collection
        (
            self.data_path,
            self.data_path_raw,
            self.data_path_processed,
            self.data_path_reports,
            self.copyright_path,
            self.digitization_path,
            self.authorities_path,
            self.aleph_custom21_path,
            self.aleph_manage18_path,
            self.aleph_custom04_path,
        ) = create_directory(CMS, self.BASE_PATH)

        print(
            self.data_path,
            "\n",
            self.data_path_raw,
            "\n",
            self.data_path_processed,
            "\n",
            self.data_path_reports,
            "\n",
            self.copyright_path,
            "\n",
            self.digitization_path,
            "\n",
            self.authorities_path,
            "\n",
            self.aleph_custom21_path,
            "\n",
            self.aleph_manage18_path,
            "\n",
            self.aleph_custom04_path,
        )

        if (sys.argv[0]).endswith("preprocess_1.py"):

            while not file_907_exists(self.aleph_custom04_path, collection_id):

                sys.stderr.write(
                    f"no {collection_id}_alma_sysno.xlsx exists. \n Running preprocess_0 first"
                )
                preprocess_0.main(collection_id=self.collection_id, branch=self.branch)

        # set up logger for collection instance
        logger = logging.getLogger(__name__)

        if manual:
            (
                client,
                self.google_sheet_file_id,
                self.google_sheet_file_name,
            ) = choose_catalog_gspread(connect_to_google_drive(), self.collection_id)
        else:
            (
                client,
                self.google_sheet_file_id,
                self.google_sheet_file_name,
            ) = find_last_updated_gspread(
                connect_to_google_drive(), self.set_collection_id()
            )

        logger.info("Creating ")

        self.dfs = create_xl_from_gspread(client, self.google_sheet_file_id)
        export_entire_catalog(self, self.dfs, stage="PRE_FINAL")

        self.df_catalog = remove_instructions_row(remove_empty_rows(self.dfs["קטלוג"]))
        self.df_collection = remove_instructions_row(
            remove_empty_rows(self.dfs["אוסף"])
        )

        if self.branch != "REI":
            self.df_collection = add_current_owner(
                self.df_collection, Authority_instance.df_credits, self.collection_id
            )
            # self.df_personalities = remove_instructions_row(
            #     remove_empty_rows(self.dfs["אישים"])
            # )
            # self.df_corporation = remove_instructions_row(
            #     remove_empty_rows(self.dfs["מוסדות"])
            # )
            # try:
            #     if self.branch != "VC-Design" and self.branch != "Design":
            #         work_col = [x for x in self.dfs.keys() if "יצירות" in x][0]
            #         self.df_works = self.dfs[work_col]
            # except:
            #     pass

        self.full_catalog = self.make_one_table()

        self.all_tables = [
            type(getattr(self, name)).__name__
            for name in dir(self)
            if name[:2] != "__" and name[-2:] != "__"
        ]

        if "קטלוג סופי" in self.dfs.keys():
            # if inspect.stack()[1] == "preprocess_1":
            #     breakpoint

            self.df_final_data = remove_unnamed_cols(
                self.dfs["קטלוג סופי"].rename(
                    columns={"Unnamed: 1": "סימול", "": "mms_id"}
                )
            )
            print(
                "\n".join(
                    [
                        f"{i} :{col}"
                        for col, i in enumerate(self.dfs["קטלוג סופי"].columns)
                    ]
                )
            )
            # print(f'column of קטלוג סופי are: {[index, col for (index, col) in enumrate(self.dfs.columns)]}'
            self.df_final_data.rename(
                columns={self.df_final_data.columns[0]: "mms_id"}, inplace=True
            )
            self.df_final_data = self.df_final_data.set_index("mms_id")

        self.full_catalog = add_missing_cols_to_table(self.full_catalog)

        # turn headers to English
        logger.info(f"Creating Excel: Saving file ")
        export_entire_catalog(self, self.dfs, stage="PRE1_FINAL")
        self.create_catalog_metadata_file()

    def create_MARC_XML(self):
        """
        Creates a MARC XML format file from the given dataframe
        :return:
        """
        df = self.marc_data
        #  MARCXML file
        output_file = self.data_path_processed / (
            self.collection_id + "_final_" + self.dt_now + ".xml"
        )
        writer = XMLWriter(open(output_file, "wb"))

        # MarcEdit MRK file
        output_file_mrk = self.data_path_processed / (
            self.collection_id + "_finalMRK_" + self.dt_now + ".txt"
        )
        mrk_file = open(output_file_mrk, "w", encoding="utf8")

        start_time = time.time()
        counter = 1

        for index, row in df.iterrows():

            record = Record()

            # add control field
            record.add_field(Field(tag="001", data=str(index)))

            for col in df:
                # if field is empty, skip
                if str(row[col]) == "":
                    continue
                # leader
                elif col == "LDR":
                    l = record.leader
                    l.record_status = "c"  # c - Corrected or revised
                    l.type_of_record = "p"  # p - Mixed materials

                    # Bibliographic level
                    if row["351"] == "File Record" or "Item Record":
                        l.bibliographic_level = "c"
                    else:
                        l.bibliographic_level = "d"

                    l.coding_scheme = "a"  # flag saying this record is utf8
                    l.cataloging_form = "a"
                    continue

                # 008
                elif col == "008":
                    field = Field(tag="008", data=row[col])
                    record.add_field(field)
                    continue

                # extract field name
                field = col[:3]

                # extract indicators
                if col.find("_") == -1:
                    col_name = "{:<5}".format(col)
                    ind = [col_name[3], col_name[4]]
                else:
                    col_name = "{:<5}".format(col[: col.find("_")])
                    ind = [col_name[3], col_name[4]]

                # extract sub-fields
                subfields_data = list()
                subfields_prep = list(filter(None, str(row[col]).split("$$")))
                for subfield in subfields_prep:
                    if subfield == "":
                        continue
                    subfields_data.append(subfield[0])
                    subfields_data.append(subfield[1:])

                # print('field:', field)
                # if not ind:
                #     print('no indicators')
                # else:
                #     print('indicators:', ind)
                # # print('subfields:', subfields_data)

                record.add_field(
                    Field(tag=field, indicators=ind, subfields=subfields_data)
                )

            counter += 1
            # mrk_file.write(record.as_marc())
            writer.write(record)
        writer.close()
        mrk_file.close()
        run_time = time.time() - start_time

        return counter, run_time

    def create_marc_seq_file(self):
        """
        function to transform a MARC formatted Dataframe into a MARC sequantial file

        """
        logger = logging.getLogger(__name__)
        logger.info(
            f"[MARC Sequantial] Creating MARC sequantial file for {self.collection_id}"
        )

        df = self.marc_data
        ad = AlphabetDetector()
        output_file_name = self.data_path_processed / (
            self.collection_id + "_final_" + self.dt_now + ".txt"
        )

        with open(output_file_name, "w", encoding="utf8") as f:
            for index, row in df.iterrows():

                f.write(f"{index} LDR   {row['LDR']}\n")
                f.write(f"{index} 001   {index}\n")

                for col in df:
                    # if field is empty, skip
                    if str(row[col]) == "" or col == "LDR":
                        continue

                    # # check language
                    # lang = ad.detect_alphabet(str(row[col]))
                    # if "HEBREW" in lang:
                    #     lang = "H"
                    # else:
                    #     lang = "L"

                    # construct 5 character field code
                    if "_" in col:
                        col_name = "{:<5}".format(col[: col.find("_")])
                    else:
                        col_name = "{:<5}".format(col)

                    # construct the line for the MARC sequantial file
                    line = f"{index} {col_name} {str(row[col])}\n"
                    # if col_name == '035':
                    line = line.replace("$$$$", "$$")
                    line = line.replace("$$a$$a", "$$")

                    # write to file
                    f.write(line)

    def set_branch(self):
        while True:
            branch: str = input(
                "Please enter the name of the Branch (Architect, Design, Dance, Theater): "
            )
            branch = str(branch)
            if branch[0].islower():
                branch = branch.capitalize()
            if branch.upper() == "REI":
                branch = branch.upper()
                break
            if branch not in Collection._project_branches:
                print("need to choose one of: Architect, Design, Dance, Theater")
                continue
            else:
                # we're happy with the value given.
                branch = "VC-" + branch
                break

        self.branch = branch

    def set_collection_id(self):
        while True:
            collection_id = input("please enter the Collection ID:")
            if not collection_id:
                print("Please enter a collection ID.")
            else:
                # we're happy with the value given.
                break
            self.collection_id = collection_id

    def set_cms(self):
        while True:
            CMS = input("please enter the name of the target CMS (Aleph, Alma):")
            if not CMS or CMS.capitalize() not in ["Aleph", "Alma"]:
                print("Please enter the name of the target CMS (Aleph, Alma).")
            else:
                # we're happy with the value given.
                break
            self.CMS = CMS


def write_log(text, log_file):
    f = open(log_file, "a")  # 'a' will append to an existing file if it exists
    log_line = "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] {}".format(text)
    f.write("{}\n".format(text))  # write the text to the logfile and move to next line
    return
