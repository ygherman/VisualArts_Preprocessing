import logging
import sys
from pathlib import Path
from xml.dom import minidom

import pandas as pd

from VC_collections import Collection
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
    try:
        if file_path.exists():
            logger.debug(
                "File already exists. changing name of file to ['sysno_old.xlsx']"
            )
            file_path_new = Path(str(file_path).replace("sysno.xlsx", "sysno_old.xlsx"))
            file_path.rename(file_path_new)
    except:
        logger.debug(
            "'sysno_old.xlsx' File already exists. changing name of file to ['sysno_old1.xlsx']"
        )

        file_path_new = Path(str(file_path).replace("sysno.xlsx", "sysno_old1.xlsx"))
        file_path.rename(file_path_new)


def main():

    logger = logging.getLogger(__name__)
    collection = Collection.retrieve_collection()
    """ initialize logger for the logging file for that collection"""
    initialize_logger(collection.branch, collection.collection_id)

    logger.info("")
    rosetta_file_path = minidom.parse(
        lookup_rosetta_file(collection.digitization_path, collection.collection_id)
    )
    logger.info(
        "Extracting 001 (MMSID) and 093 (*)Call number) and turning into Dataframe"
    )
    d = create_mmsid_dict(rosetta_file_path)

    df = pd.DataFrame(d).transpose()
    df.index = df.index.astype(str)
    df.index.name = "mmsid"

    file_full_path = collection.aleph_custom04_path / (
        collection.collection_id + "_alma_sysno.xlsx"
    )
    check_custom04_file(file_full_path)

    logger.info("Saving Dataframe to Excel file in the custom04 folder")
    df.to_excel(file_full_path)


if __name__ == "__main__":
    while True:
        main()
        batch = input("Run another collection through Preprocess-1? (Y/N) ")
        if batch.strip().lower() != "y":
            sys.stdout.write("Ending run!")
            sys.exit()
