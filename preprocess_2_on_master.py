import sys
import time
import timeit

from VC_collections.logger import initialize_logger_for_master_process2
from VC_collections.value import *
from VC_collections.authorities import *
from VC_collections.AuthorityFiles import Authority_instance

files = {
    "Architect": "1EhFxuphaOu-S8rZy_7O8OuLk3BMH_rAXjOTqDg5yMvY",
    "Dance": "1WcqUNkDhNbDyTdYLU_S3TbVkjZ6G03m36_9FqvBgaSc",
    "Design": "1M-vegxBcBI7J5hZvoqLkQQ7ATxMmfyyrFpAX4-vR7FM",
    "Theater": "1goBGosdUcCLzmdxIuF1SRYgk_mIT-eIsCJgdvd69rCw",
}


def find_collection_id(call_number):
    if call_number.count("-") > 0:
        if "IL" in call_number and call_number.count("-") == 1:
            return call_number
        elif "IL" in call_number and call_number.count("-") > 1:
            return call_number[: find_nth(call_number, "-", 2)]
        else:
            return call_number[: call_number.find("-")]
    else:
        return call_number


def create_dataframe(client, branch):
    return pd.DataFrame(
        Collection.create_xl_from_gspread(client, files[branch])["Master Catalog"]
    )


def create_MARC_LDR(level):
    if level == "File Record" or level == "Item Record":
        return "00000npd#a22######a#4500"
    else:
        return "00000npc#a22########4500"


def create_MARC_351(df):
    df["351"] = df["רמת תיאור"].apply(lambda x: "$$c" + str(x).strip())

    return df


def create_MARC_093(df):
    # creating $c tag
    df["093"] = df["סימול"].apply(lambda x: "$$c" + str(x).strip())

    # creating $d tag

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

    return df


def main():
    start_time = timeit.default_timer()

    branch = input("Please enter branch name: [Architect, Dance, Design, Theater]\n")

    """ initialize logger for the logging file for that collection"""
    initialize_logger_for_master_process2(branch)
    logger = logging.getLogger(__name__)

    logger.info(f"\n Starting new preprocess of {branch}, at: {datetime.now()}")
    logger.info(
        f'\nStarting new preprocess {"/".join(str(sys.modules[__name__])[:-1].split("/")[-3:])} of '
        f"{branch}, at: {datetime.now()}"
    )
    time.sleep(0.5)

    # connect to google drive and create the dataframe
    google_client = Collection.connect_to_google_drive()
    df_catalogs = create_dataframe(google_client, branch=branch)

    # create LDR
    df_catalogs["LDR"] = df_catalogs["רמת תיאור"].apply(create_MARC_LDR)

    # create 351 (רמת תיאור)
    logger.info(f"[351] Creating  MARC 351 - LEVEL OF DESCRIPTION")
    df_catalogs = create_MARC_351(df_catalogs)

    # create MARC 093 field for Call Number (סימול פרויקט)
    logger.info("[093] Creating 911/093 MARC field for Call Number")
    df_catalogs = create_MARC_093(df_catalogs)

    # # Add MMS id to catalog (מספר מערכת עלמא)
    # logger.info("[001] Add MMS id to catalog")
    # collection.df_final_data = drop_col_if_exists(collection.df_final_data, 'mms_id')
    # collection.df_final_data, df_alma = project.get_alma_sid(
    #     collection.aleph_custom04_path,
    #     collection.collection_id,
    #     collection.df_final_data,
    # )

    # create MARC 091
    logger.info(f"[091] Create MARC 091 Field")
    collection.df_final_data = marc.create_MARC_091(collection.df_final_data)

    # create 008
    logger.info(f"[008] Creating  MARC 008 field")
    collection.df_final_data = marc.create_MARC_initial_008(collection.df_final_data)

    # create 520 (תיאור)
    logger.info(f"[520] Creating  MARC 520 - SCOPE AND CONTENT")
    collection.df_final_data = marc.create_MARC_520(collection.df_final_data)

    # create 245 (כותרת)
    logger.info(f"[245] Creating  MARC 245 - UNITITLE")
    collection.df_final_data = marc.create_MARC_245(collection.df_final_data)

    # creator unknown, multiple creators
    logger.info("[MARC 952$g multiple and unknown creators] creating 952$g")
    collection.df_final_data = marc.create_MARC_952_mul_unknown_creators(
        collection.df_final_data
    )

    # create 110 and 100 (FIRST CREATORS CORPS and PERS) (יוצר ראשון - איש/ יוצר ראשון = מוסד)
    logger.info("[MARC 100/110] Creating  MARC 100/110 - Creators (first)")
    collection.df_final_data = marc.create_MARC_100_110(collection.df_final_data)

    # create 300 (EXTENT) (היקף)
    logger.info("[MARC 300] Creating  MARC 300 - EXTENT ")
    collection.df_final_data = marc.create_MARC_300(collection.df_final_data)

    # create 700 and 710 (added creators PERS and CORPS) (יוצרים נוספים - אישים/יוצרים נוספים - מוסד)
    logger.info(
        "[MARC 700/710] Creating  MARC 700/710 - Personlities and Corporate bodies access points"
    )
    collection.df_final_data = marc.create_MARC_700_710(
        collection.df_final_data, Authority_instance.df_credits
    )

    # create 535 (EXTENT) (היקף)
    logger.info("[MARC 535] Creating  MARC 535 - location of originals ")
    collection.df_final_data = marc.create_MARC_535(collection.df_final_data)

    logger.info("[MARC 306] Creating  MARC 306 -  duration")
    collection.df_final_data = marc.create_MARC_306(collection.df_final_data)

    # create 655 (ARCHIVAL_MATERIAL) (סוג חומר)
    logger.info("[MARC 655] Creating  MARC 655 - ARCHIVAL MATERIAL ")
    collection.df_final_data = marc.create_MARC_655(collection.df_final_data)

    # create 630 (WORKS) (יצירות)
    logger.info("[MARC 630] Creating  MARC 630 - WORKS ")
    collection.df_final_data = marc.create_MARC_630(collection.df_final_data)

    # create 041 (LANGUAGE) (שפה)
    logger.info("[MARC 041] Creating  MARC 041 - LANGUAGE")
    collection.df_final_data = marc.create_MARC_041(collection.df_final_data)

    ####################################################
    ### CREATE  COPYRIGHT FIELDS WITH DEFAULT VALUES ###
    ### fields: 939, 903, 952
    ####################################################
    logger.info("[MARC 939, 903, 952] Creating default copyright fields")

    if str(input("Copyright Analysis already done? (Y/N)")).lower() == "n":

        copyright_analysis_done = False
    else:
        copyright_analysis_done = True

    if not copyright_analysis_done:
        collection.df_final_data = marc.create_MARC_default_copyright(
            collection.df_final_data
        )
    else:
        collection = marc.add_copyright_field_from_alma(collection)

    collection.df_final_data = marc.create_default_040(collection.df_final_data)

    # create 255 - scale field
    logger.info("[MARC 255] Creating  MARC 255 - SCALE")
    collection.df_final_data = marc.create_MARC_255(collection.df_final_data)

    # create 260 (DATE fields, and PUBLICATION_COUNTRY) (מדינת פרסום, תאריך מנורמל מוקדם, תאריך מנורמל מאוחר)
    logger.info(
        "[MARC 260/008 DATES] Creating  MARC 260 $g $e - DATE (free text), and publication country."
        " Updates MARC 008"
    )
    collection.df_final_data = marc.create_MARC_260_008_date(
        collection.df_final_data,
        "תאריך מנורמל מוקדם",
        "תאריך מנורמל מאוחר",
        "תאריך חופשי",
    )

    # create 260 (DATE fields, and PUBLICATION_COUNTRY) (מדינת פרסום, תאריך מנורמל מוקדם, תאריך מנורמל מאוחר)
    logger.info(
        "[MARC 260/008/044 COUNTRIES] Creating  MARC 260 $g $e - DATE (free text), and publication country."
        " Updates MARC 008"
    )
    collection.df_final_data = marc.create_MARC_260_044_008_countries(
        collection.df_final_data, "מדינת הפרסום/הצילום"
    )
    if not copyright_analysis_done:
        logger.info("[MARC 952] Creating MARC 952 - Privacy")
        collection.df_final_data = marc.create_MARC_952(collection.df_final_data)

    # add 597 (CREDIT)
    collection = marc.add_MARC_597(collection)
    logger.info("[MARC 597] Creating MARC 597 - CREDITS")

    # add 524 (CREDIT)
    logger.info("[MARC 524] Creating MARC 597 - CREDITS")

    collection = marc.add_MARC_597(collection)

    # create 921, 933 (CATALOGUER, CATALOGING DATE)
    if collection.branch != "REI":
        logger.info(
            "[MARC 921/933] Creating MARC 921/933 - CATALOGUERS and CATALOGUING DATE"
        )

        collection.df_final_data = marc.create_MARC_921_933(collection.df_final_data)

    # create 500 (NOTES) and other fields:
    logger.info("[MARC 500] Creating MARC 500 - NOTES")
    collection.df_final_data = marc.create_MARC_500(collection.df_final_data)
    collection.df_final_data = marc.create_MARC_500s_4collection(
        collection.df_final_data
    )

    # create 999 (Default values: NOULI, NOOCLC, ARCHIVE)
    logger.info(
        "[MARC 999] initializing MARC 999 with constant values: NOULI, NOOCLC, ARCHIVE"
    )
    collection.df_final_data = marc.create_MARC_999(collection.df_final_data)

    # create BAS=VIS, in alma BAS -> 906
    logger.info("[MARC 906] Adding BAS = VIS - in Alma 906")
    collection.df_final_data = marc.create_MARC_BAS(collection.df_final_data)

    # create FMT
    collection.df_final_data = marc.create_MARC_FMT(collection.df_final_data)

    # create OWN (Default value: NNL)
    logger.info(
        "[MARC 948] initializing MARC 948 - formerly Own with constant values: NNL"
    )
    collection.df_final_data = marc.create_MARC_948(collection.df_final_data)

    # create 773 (former LKR)
    logger.info("[MARC 773] Creating MARC 773 - the hierarchical link field")
    collection.df_final_data = marc.create_MARC_773(collection.df_final_data)

    # create 336
    logger.info("[MARC 336] Creating MARC RDA 336 ")

    collection.df_final_data, df_explode_336 = marc.create_MARC_336(
        collection.df_final_data
    )
    df2 = pd.concat([collection.df_final_data, df_explode_336], axis=1)

    # create 337 338
    logger.info("[MARC 337/338] Creating MARC RDA 337/338 ")
    collection.df_final_data = marc.create_MARC_337_338(collection.df_final_data)

    # create 534
    logger.info("[MARC 534] Creating MARC 534 - MEDIA FORMAT ")
    collection.df_final_data = marc.create_MARC_534(collection.df_final_data)

    # create MARC 590
    logger.info("[MARC 590] Creating MARC  590  - HIDDEN NOTES")
    collection.df_final_data = marc.create_MARC_590(
        collection.df_final_data, copyright_analysis_done
    )

    # create MARC 9421 (formerly 561)
    logger.info("[MARC 942] Creating MARC  942 - Ownership and Origial Call number")
    collection.df_final_data = marc.create_MARC_942(
        collection.df_final_data, collection.collection_id
    )

    # collection.df_final_data = marc.create_MARC_561(collection.df_final_data)

    collection.temp_preprocess_file(stage="POST")

    #  ADD 907 (#Rossetta link)
    logger.info(
        "[MARC 907] Recreating MARC 907 - adding the Rossetta field, link to the digital object (if exists)"
    )
    if input("Add 907 to collection? (y/n)").lower == "y":
        collection = marc.add_MARC_907(collection)

    # recreate 035 MARC field from the ROS\[collection_id]_907.xml file
    logger.info(
        "[MARC 035] Recreating MARC 035 - for records which are migrated from Aleph"
    )
    collection = marc.add_MARC_035(collection)

    # create MARC 650 for project branches
    logger.info(
        "[MARC 650] create MARC 650 subject heading according to collection's branch"
    )
    collection = marc.create_MARC_650_branch(collection)

    # last text cleaning up of dataframe:
    collection.df_final_data = collection.df_final_data.replace(
        r"\\n", " ", regex=True
    ).replace("  ", " ", regex=True)

    # create MARC Catalog
    marc.export_MARCXML_final_table(collection)
    collection.create_marc_seq_file()

    #

    ###############################################
    ### export final dataframe to check process ###
    ###############################################
    collection.temp_preprocess_file(stage="POST")

    ###############################################
    ###      how much time the process ran?     ###
    ###############################################
    elapsed = timeit.default_timer() - start_time
    logger.info(f"Execution Time: {elapsed}")


if __name__ == "__main__":
    while True:
        main()
        batch = input("Run another collection through Preprocess-2? (Y/N)")
        if batch.lower() != "y":
            sys.stdout.write("Ending run!")
            sys.exit()
        logger.handlers = []
        logging.shutdown()
