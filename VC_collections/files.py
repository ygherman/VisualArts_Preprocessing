import datetime
import os
import platform
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# def get_google_drive_api_path(path):
#     parent = path.parent
#     for x in parent.iterdir():
#         if x.is_dir() and x != path:
#             yield x


def make_sure_path_exists(path):
    """
    Make sure Path exists. raises an exception if path doesn't exist

    :param path: the path to check
    """

    if not os.path.exists(path):
        os.makedirs(path)


def create_directory(CMS, BASE_PATH):
    """
        creates Directory structure
    :param CMS: the brach name (Architect, Dance, Design or Theater)
    :param BASE_PATH: the ID of the collection
    :return: returns the paths to the directories that were created:
            data_path, data_path_raw, data_path_processed,
            data_path_reports, copyright_path, digitization_path,
            authorities_path, aleph_custom21, aleph_manage18, aleph_custom04

    """
    # create a Data Folder

    print("BASE_PATH", BASE_PATH)
    data_path = BASE_PATH / "Data"
    make_sure_path_exists(data_path)

    # create a Data Folder
    data_path_raw = data_path / "raw"
    make_sure_path_exists(data_path_raw)

    # create a Data Folder
    data_path_processed = data_path / "processed"
    make_sure_path_exists(data_path_processed)

    # create a Data Folder
    data_path_reports = data_path / "reports"
    make_sure_path_exists(data_path_reports)

    # create a Copyright folder
    copyright_path = BASE_PATH / "Copyright"
    make_sure_path_exists(copyright_path)

    # create a Digitization folder
    digitization_path = BASE_PATH / "Digitization"
    make_sure_path_exists(digitization_path)

    # create a Digitization folder
    ROS_path = digitization_path / "ROS"
    make_sure_path_exists(ROS_path)

    # create a Authorities folder
    authorities_path = BASE_PATH / "Authorities"
    make_sure_path_exists(authorities_path)

    # create a custom21 folder
    aleph_custom21 = BASE_PATH / "Custom21"
    make_sure_path_exists(aleph_custom21)

    # create a custom21 folder
    aleph_manage18 = BASE_PATH / "Manage18"
    make_sure_path_exists(aleph_manage18)

    # create a custom04 folder
    if CMS == "aleph":
        aleph_custom04 = BASE_PATH / "Custom04"
    else:
        aleph_custom04 = BASE_PATH / "Custom04" / "Alma"
    make_sure_path_exists(aleph_custom04)

    var = (
        data_path,
        data_path_raw,
        data_path_processed,
        data_path_reports,
        copyright_path,
        digitization_path,
        authorities_path,
        aleph_custom21,
        aleph_manage18,
        aleph_custom04,
    )
    return var


def has_sheet(spreadsheet, sheet_name):
    for x in spreadsheet.worksheets():
        if sheet_name == x.title:
            return True
    return False


def get_collections_in_folder(root_folder):
    """
    :param root_folder: the root folder in which all the branches directories are stores
    :return: a dictionary of collections with the branches a keys

    """
    branches = ["Architect", "Dance", "Design", "Theater"]
    collections = dict()

    # initiate the dictionary with the 4 branches as keys
    for br in branches:
        collections[br] = list()

    for branch in branches:
        path = root_folder / ("VC-" + branch)
        for file in path.glob("/*"):
            if "." in os.path.basename(file):
                continue
            collections[branch].append(os.path.basename(file))
    return collections


def find_newest_file(path, file_name_pattern, mode="start"):
    """
    Function that gets a string pattern with a selected mode
        start - looking for the string pattern at the beginning of the file name
        mid - looking for the string pattern at the middle of the file name
        end - looking for the string pattern at the end of the file name

    :param path: the path to the place to look for the file
    :param mode: the mode to look for the string pattern in the file name (beginning/middle/end of name)
    :param file_name_pattern: the string pattern to look for

    :returns the newest file by creation date property of the file

    """
    global files
    try:
        if mode == "start":
            files = [
                filename
                for filename in os.listdir(path)
                if filename.startswith(file_name_pattern)
            ]
        elif mode == "mid":
            files = [
                filename
                for filename in os.listdir(path)
                if file_name_pattern in filename
            ]
        elif mode == "end":
            files = [
                filename
                for filename in os.listdir(path)
                if filename.endswith(file_name_pattern)
            ]
    except OSError:
        print("no file name matching pattern found in path: {}".format(path))
    finally:
        pass

    # find the most updated file
    file_dates = []

    for index, file in enumerate(files):
        if file.startswith("~$"):
            continue
        created = os.stat(os.path.join(path, file)).st_ctime
        file_dates.append(datetime.datetime.fromtimestamp(created))
    if len(file_dates) > 0:
        index, file = max(enumerate(file_dates))
        print(files[index])
        return files[index]
    else:
        return ""


def get_creation_date(path_to_file):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    :param path_to_file: the path to the file
    :return: creation date of the file
    """
    if platform.system() == "Windows":
        return os.path.getctime(path_to_file)
    else:
        stat = os.stat(path_to_file)
        try:
            return stat.st_birthtime
        except AttributeError:
            pass
    # We're probably on Linux. No easy way to get creation dates here,
    # so we'll settle for when its content was last modified.


def find_newest_file_in_list(
    files_list: list, file_name_pattern: str, mode: str = "start"
) -> object:
    """

    @param files_list: list of file to search
    @param file_name_pattern: the string pattern for the file name to search for
    @param mode: where in the file name this patern should be found: start, mid, end.
    @return: the name of the last updated file
    """
    try:
        if mode == "start":
            files = [
                filename
                for filename in files_list
                if filename.lower().startswith(file_name_pattern.lower())
            ]

        elif mode == "mid":
            files = [
                filename
                for filename in files_list
                if file_name_pattern.lower() in filename.lower()
            ]
        elif mode == "end":
            files = [
                filename
                for filename in files_list
                if filename.lower().endswith(file_name_pattern.lower())
            ]
    except OSError:
        print(f"no file name matching pattern found in files list")
    finally:
        pass

    # find the most updated file
    file_dates = []

    for index, file in enumerate(files):
        if file.startswith("~$"):
            continue
        created = file[-8:]
        file_dates.append(datetime.datetime.strptime(created, "%Y%m%d"))
    if len(file_dates) > 0:
        index, file = max(enumerate(file_dates))
        print(files[index])
        return files[index]
    else:
        return ""


def find_newest_file_in_folder(path, file_name_pattern, mode="start"):
    """
    Function that gets a string pattern with a selected mode
        start - looking for the string pattern at the beginning of the file name
        mid - looking for the string pattern at the middle of the file name
        end - looking for the string pattern at the end of the file name

    :param path: the path to the place to look for the file
    :param mode: the mode to look for the string pattern in the file name (beginning/middle/end of name)
    :param file_name_pattern: the string pattern to look for

    :returns the newest file by creation date property of the file

    """
    try:
        if mode == "start":
            files = [
                filename
                for filename in os.listdir(path)
                if filename.startswith(file_name_pattern)
            ]
        elif mode == "mid":
            files = [
                filename
                for filename in os.listdir(path)
                if file_name_pattern in filename
            ]
        elif mode == "end":
            files = [
                filename
                for filename in os.listdir(path)
                if filename.endswith(file_name_pattern)
            ]
    except OSError:
        print("no file name matching pattern found in path: {}".format(path))
    finally:
        pass

    # find the most updated file
    file_dates = []

    for index, file in enumerate(files):
        if file.startswith("~$"):
            continue
        created = os.stat(os.path.join(path, file)).st_ctime
        file_dates.append(datetime.datetime.fromtimestamp(created))
    if len(file_dates) > 0:
        index, file = max(enumerate(file_dates))
        print(files[index])
        return files[index]
    else:
        return ""


def is_folder_empty(path):
    """
    Function gets a folder and checks whether the folder is empty.
    :param path: path to folder
    :return: True iis the folder is empty, false is otherwise.
    """
    if path.is_dir():
        print(os.listdir(path))
        files = os.listdir(path)
        if "desktop.ini" in files:
            files.remove("desktop.ini")
        return not files
    else:
        return True


def get_file_path(stage, df, pattern=""):
    """
    looks for file in a certain parent folder
    :param stage: parent folder
    :param df: the dataframe in which to store to file path
    :param pattern: string pattern of the file name to look for
    :return: return the dataframe with the
    """
    df[stage] = ""

    def check_folder(dir_path, collection_id):
        """
        a sub-action that checks whether the folder exists and whether it's  empty or not.
        :param dir_path: the path to the directory
        :param collection_id: the ID of the collection to look for. this is the root folder of the file structure.
        """
        print("is_folder_empty(dir_path):", is_folder_empty(dir_path))
        if dir_path.is_dir() and not is_folder_empty(dir_path):
            df.loc[index, stage] = dir_path / find_newest_file_in_folder(
                dir_path, collection_id + pattern, mode="mid"
            )
        else:
            df.loc[index, stage] = ""
        print(dir_path)

    if stage == "raw Data":
        for index, row in df.iterrows():
            dir_path = (
                Path.cwd()
                / ("VC-" + str(row["branch"]))
                / str(row["collection"])
                / "Data"
                / "raw"
            )
            check_folder(dir_path, str(row["collection"]))

    elif stage == "processed Data":
        for index, row in df.iterrows():
            dir_path = (
                Path.cwd()
                / ("VC-" + str(row["branch"]))
                / str(row["collection"])
                / "Data"
                / "processed"
            )
            check_folder(dir_path, str(row["collection"]))

    else:
        for index, row in df.iterrows():
            dir_path = (
                Path.cwd()
                / ("VC-" + str(row["branch"]))
                / str(row["collection"])
                / stage
            )
            check_folder(dir_path, str(row["collection"]))

    return df


def write_excel(df, path, sheets):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    """
    creates a excel file of a given dataframe
    :param df: the dateframe or a list of dataframes to write to excel
    :param path: the path name of the output file, or a list of sheets
    :param sheets: can be a list of sheet or
    """
    while True:

        try:
            writer = pd.ExcelWriter(path, engine="xlsxwriter")
            break
        except Exception as e:
            sys.stderr.write(f"Exception is {e} ")
            input("Press Enter to continue...")

    # Convert the dataframe to an XlsxWriter Excel object.
    if type(df) is dict and type(sheets) is list:
        try:
            i = 0
            for frame in df.keys():
                df[frame].to_excel(writer, sheet_name=sheets[i])
                i += 1
        except Exception as e:
            sys.stderr.write(f"Exception is {e} ")

    elif type(df) is list and type(sheets) is list:
        try:
            i = 0
            for frame in df:
                frame.to_excel(writer, sheet_name=sheets[i])
                i += 1
        except Exception as e:
            sys.stderr.write(f"Exception is {e} ")
            input("Press Enter to continue...")
    elif type(df) is pd.DataFrame and type(sheets) is str:
        df.to_excel(writer, sheet_name=sheets)
    while True:
        try:
            writer.close()
            break
        except Exception as e:
            sys.stderr.write(f"Exception is {e}")
            input("Press Enter to continue...")


def get_branch_colletionID(branch="", collection_id="", batch=False):
    """
        Get Branch and CollectionID from user
    :param branch: the branch (Architect, Dance, Design or Theater
    :param collection_id: The collection ID
    :param batch: if the calling results from a batch process
    :return: The cms, branch and the collection ID
    """
    CMS = "alma"
    if not batch:
        while True:

            branch = input(
                "Please enter the name of the Branch (Architect, Design, Dance, Theater): "
            )
            branch = str(branch)
            try:
                if branch[0].islower():
                    branch = branch.capitalize()
                if branch not in ["Dance", "Architect", "Theater", "Design"]:
                    print("need to choose one of: Architect, Design, Dance, Theater")
                    continue
                else:
                    # we're happy with the value given.
                    break
            except IndexError:
                print("Wrong value, try again!")
                continue

        while True:
            collection_id = input("please enter the Collection ID: ")
            if not collection_id:
                print("Please enter a collectionID.")
            else:
                # we're happy with the value given.
                break
    elif batch:
        return "VC-" + branch, collection_id

    return CMS, branch, collection_id


def create_df_from_gs(spreadsheet, worksheet):
    """
    Function gets name of worksheet from a Google Sheet Spreadsheet, and returns it
    as a pandas DataFrame
    :param spreadsheet:
    :param worksheet: The name of the worksheet
    :return: df - pandas Dataframe of the worksheet
             cols - list of column names
    """
    # create a dataframe from the given worksheet
    sheet = spreadsheet.worksheet(worksheet)
    dict_gs = sheet.get_all_records(head=1)
    #     pprint.pprint(dict_gs)
    df = pd.DataFrame(dict_gs)
    cols = list(dict_gs[1].keys())
    #     print(cols)

    # remove empty rows
    df.replace(np.nan, "", inplace=True)

    return df, cols
