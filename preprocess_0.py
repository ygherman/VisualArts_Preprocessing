import json
import logging
import os
import sys
import timeit
from pathlib import Path
from xml.dom import minidom
from io import FileIO

import pandas as pd

import VC_collections.Collection
from VC_collections import Collection
from VC_collections.files import create_directory
from VC_collections.logger import initialize_logger
from VC_collections.project import lookup_rosetta_file


def create_mmsid_dict(ROS_file: minidom) -> dict:
    """
        The function takes the MARCxml file of the collection, which resides in ./[branch]/[collec  tion]/Digitization/ROS
        directory, and that was parsed into a minidom xml object, extract the MMS ID (001 tag) and the 093 (Rosetta
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
            if e.attributes["tag"].value == "093":
                for sb in e.getElementsByTagName("subfield"):
                    dd["093" + sb.attributes["code"].value] = sb.childNodes[0].data
            if e.attributes["tag"].value == "911":
                for sb in e.getElementsByTagName("subfield"):
                    dd["911" + sb.attributes["code"].value] = sb.childNodes[0].data
        d[id] = dd
    return d


def check_custom04_file(file_path):
    logger = logging.getLogger(__name__)
    if file_path.exists():
        logger.debug("File already exists. Deleting and creating new file.")
        os.remove(file_path)


def create_907_json(df_collection, collection_id, digitization_path):

    with open(r"Data\VIS_full.json") as json_file:
        rosetta_dict = json.load(json_file)

    collection_907_dict = {str(k): rosetta_dict[str(k)] for k in df_collection.index.values}

    json_path = Path(digitization_path / "ROS" / (collection_id + "_907.json"))

    if json_path.exists():
        os.remove(json_path)
    with open(digitization_path / "ROS" / (collection_id + "_907.json"), "w", encoding="utf8") as fp:
        json.dump(collection_907_dict, fp)

    return df_collection


def main(**collection):
    logger = logging.getLogger(__name__)
    start_time = timeit.default_timer()

    if type(collection) == VC_collections.Collection.Collection:
        collection_id = collection.collection_id
        branch = collection.branch

    elif len(collection) != 0:
        collection_id = collection["collection_id"]
        branch = collection["branch"]

    else:
        branch = "VC-" + input(
            "Please enter the name of the Branch (Architect, Design, Dance, Theater): : "
        )
        collection_id = input("Please enter collection id: ")
    FILE_PATH = Path(r"Data\vis_full.xml")
    FILE_PATH_XML = minidom.parse(FileIO(FILE_PATH))


    # collection = Collection.retrieve_collection()
    """ initialize logger for the logging file for that collection"""
    initialize_logger(branch, collection_id)

    logger.info(
        "Extracting 001 (MMSID) and 093 (*)Call number) and turning into Dataframe"
    )


    d = create_mmsid_dict(FILE_PATH_XML)

    df = pd.DataFrame(d).transpose()
    df.index = df.index.astype(str)
    df.index.name = "mmsid"

    mask = (df["093c"].str.contains(collection_id)) | (
        df["093d"].str.contains(collection_id)
    )
    df_collection = df[mask]

    BASE_PATH = Path.cwd().parent / branch / collection_id

    (
        data_path,
        data_path_raw,
        data_path_processed,
        data_path_reports,
        copyright_path,
        digitization_path,
        authorities_path,
        aleph_custom21_path,
        aleph_manage18_path,
        aleph_custom04_path,
    ) = create_directory("Alma", BASE_PATH)

    file_full_path = aleph_custom04_path / (collection_id + "_alma_sysno.xlsx")
    check_custom04_file(file_full_path)

    logger.info("Saving Dataframe to Excel file in the custom04 folder")
    df_collection.to_excel(file_full_path)

    create_907_json(df_collection, collection_id, digitization_path)

    elapsed = timeit.default_timer() - start_time
    logger.info(f"Execution Time: {elapsed}")


if __name__ == "__main__":
    while True:
        main()
        batch = input("Run another collection through Preprocess-1? (Y/N) ")
        if batch.strip().lower() != "y":
            sys.stdout.write("Ending run!")
            sys.exit()
