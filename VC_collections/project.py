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
    Date: 30/07/2019 13:04

    $
"""
import logging
import os
import sys
from pathlib import Path

import pandas as pd

# ROOTID finder
from .columns import drop_col_if_exists
from .value import find_nth

ROOTID_finder = lambda x: x[: find_nth(x, "-", x.count("-"))] if "-" in x else ""


def get_aleph_sid(custom04_path, collectionID, df):
    """
        Get Aleph sys ID
    :param custom04_path: the path to the file with the aleph system ID's
    :param collectionID: the collection ID
    :param df: the dataframe to add the aleph system number to
    :return: the Dataframe with a system number column
    :return:
    """
    aleph_sysno_file = os.path.join(custom04_path, collectionID + "_aleph_sysno.xlsx")
    assert os.path.isfile(aleph_sysno_file), "There is no such File: aleph_sysno_file"

    # parse sysno file
    xl2 = pd.ExcelFile(aleph_sysno_file)
    df_aleph = xl2.parse("Sheet1")

    # rename columns
    df_aleph = df_aleph.rename(columns={"Adlib reference (911a)": "911##a"})

    df_aleph = df_aleph.set_index(list(df_aleph)[1])
    df_aleph.index.names = ["סימול"]
    df_aleph = df_aleph.iloc[:, 0:1]
    df_aleph.columns = ["System number"]

    df = df.join(df_aleph, how="left")

    return df, df_aleph


# TODO refactor this function
def all_records_in_alma(df):
    return len(df[df["_merge"] == "left_only"]) == 0


def get_alma_sid(custom04_path, collectionID, df):
    """
        Get Aleph sys ID
    :param custom04_path: the path to the file with the aleph system ID's
    :param collectionID: the collection ID
    :param df: the dataframe to add the aleph system number to
    :return: the Dataframe with a system number column
    :return:
    """
    try:
        alma_sysno_file = os.path.join(custom04_path, collectionID + "_alma_sysno.xlsx")
    except:
        sys.stderr.write(
            f"There is no alma_sysno_file File for collection: {collectionID}."
        )

    # parse sysno file
    try:
        xl2 = pd.ExcelFile(alma_sysno_file)
    except FileNotFoundError:
        sys.stderr.write(
            f"The file does [{alma_sysno_file}] not exist in the custom04\Alma directory. "
            f"Search or create the file and restart process."
        )
        sys.exit()
    df_alma = xl2.parse(0)
    df_alma = df_alma.applymap(str)

    df_alma = df_alma.set_index(list(df_alma.columns)[1])
    df_alma.index.names = ["סימול"]
    df_alma = df_alma.iloc[:, 0:1]

    # rename columns
    df_alma.columns = ["MMS ID"]

    # convert MMS ID col to string
    df.index = df.index.str.strip()
    df = df.merge(df_alma, on="סימול", how="left", indicator=True)
    # todo change this to logger

    df = drop_col_if_exists(df, "911_1")
    try:
        df["MMS ID"] = df["MMS ID"].astype(str)
    except ValueError:
        sys.stderr.write(
            f"There is a missing MMS ID at {df.loc[df['MMS ID'].isna() == True, 'סימול'].index}."
            f"\n Please update the Alma MMS ID for that call number and run again!"
        )
        sys.exit()

    df = df.reset_index()
    df = drop_col_if_exists(df, "911_1")
    df = df.set_index("MMS ID")
    df.index.name = "001"
    df = drop_col_if_exists(df, "911_1")

    if all_records_in_alma(df):
        df = drop_col_if_exists(df, "_merge")
        return df, df_alma, None
    else:
        new_records_to_alma = df[df["_merge"] == "left_only"]
        new_records_to_alma = drop_col_if_exists(new_records_to_alma, "_merge")

    return df, df_alma, new_records_to_alma


def get_branch_colletionID(
    branch: str = "", collectionID: str = "", batch: bool = False
) -> (str, str, str):
    """
        Get Branch and CollectionID
    :param branch: the branch (Architect, Dance, Design or Theater
    :param collectionID: The collection ID
    :param batch: if the calling results from a batch process
    :return: tuple of 3 containing CMS, branch and the collection ID
    """
    if not batch:
        while True:
            CMS = "Alma".lower()

            branch = input(
                "Please enter the name of the Branch (Architect, Design, Dance, Theater): "
            )
            branch = str(branch)
            if branch[0].islower():
                branch = branch.capitalize()
            if branch.upper() == "REI":
                branch = branch.upper()
                break
            if branch not in ["Dance", "Architect", "Theater", "Design"]:
                print("need to choose one of: Architect, Design, Dance, Theater")
                continue
            else:
                # we're happy with the value given.
                branch = "VC-" + branch
                break

        while True:
            collectionID = input("please enter the Collection ID:")
            if not collectionID:
                print("Please enter a collectionID.")
            else:
                # we're happy with the value given.
                break
    elif batch:
        return "VC-" + branch, collectionID

    return CMS, branch, collectionID


def get_root_index_and_title(index: str, df: pd.DataFrame) -> tuple:
    """
        Get the title of the parent record
    :param index: the index for which the rootid (parent id) should be looked for.
    :param df: The whole dataframe
    :return:
    """
    logger = logging.getLogger(__name__)

    if "סימול" in list(df.columns):
        check_col_series = df["סימול"].tolist()
    else:
        logger.error("[ERROR] no [סימול] column in table, check table")
        sys.exit()

    # root_call_number = ROOTID_finder(df.loc[index, "סימול"])
    # root_index = df.index[df["סימול"] == root_call_number].tolist()[0][
    root_call_number = df.loc[index, "סימול אב"].strip()

    if root_call_number in check_col_series:
        root_index = df.index[df["סימול"] == root_call_number][0]
        title = df.loc[root_index, "24510"].strip("$$a")
    else:
        logger.error(
            f"ROOT MMS ID of {index} is not in table - check table! and run again"
        )
        sys.exit()

    return root_index, title.strip()


def get_collection_paths(collectionID):
    """
        for a given collection, return all paths of the directory structure
    :param collectionID:
    """

    return ""


def lookup_rosetta_file(digitization_path, collection_id):
    rosetta_file_path = str(digitization_path / "ROS" / (collection_id + "_907.xml"))
    if not Path.exists(Path(rosetta_file_path)):
        sys.stderr.write(
            f"[ERROR] no file at {rosetta_file_path} - please add file and run again!"
        )
        sys.exit()
    return rosetta_file_path
