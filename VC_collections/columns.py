import collections
import logging
import pprint

import numpy as np
import pandas as pd

from .fieldmapper import field_types_dict


def column_exists(df, col):
    """
    checks whether a column exists in a given dataframe
    :param df: the dataframe
    :param col: the column name to check
    :return:
    """
    return col in list(df.columns)


def more_than_one_value_in_cell(df, col_name) -> bool:
    return df[col_name].str.contains(";").any()


def remove_duplicate_in_column(df, col):
    """
    check for duplicate values for each row in a Column 'col' of a dataframe 'df'
    :param df: the dataframe to check
    :param col: the column in which to search form duplicate values
    :return: the duplicate free dataframe
    """
    logger = logging.Logger(__name__)
    if col not in list(df.columns):
        return df
    for index, frame in df[col].iteritems():

        # if there is a list of values delimited by ;
        if ";" in str(frame):
            old_values_list = frame.split(";")
            # check if there are duplicate values
            #             print('len old:', len(old_values_list), 'len new:',len(set(old_values_list)))
            if len(old_values_list) > len(set(old_values_list)):
                logger.info(f"[{col}] Removing duplicates in {col} field. ")

                print("There are duplicates in column {}, index {}".format(col, index))
                pprint.pprint(
                    [
                        item
                        for item, count in collections.Counter(old_values_list).items()
                        if count > 1
                    ]
                )
                new_values_list = ";".join(list(set(old_values_list)))

            else:
                new_values_list = ";".join(old_values_list)

        # if there is only 1 value
        else:
            new_values_list = frame

        # update the column with the new list of unique values per cell
        df.loc[index, col] = new_values_list

    return df


def dupCheck(df, column_name):
    """
    Checks for duplicate values in a Dataframe df, in a specific column_name.
    :param df:
    :param column_name:
    :return: A dataframe of all duplicated/non-unique values of the string "no non-unique values found"
    """
    try:
        return pd.concat(g for _, g in df.groupby(column_name) if len(g) > 1)
    except:
        return "no non-unique values found"


def is_column_empty(df, col):
    """
    Checks that column is not empty
    :param df: the dataframe
    :param col: the column name to check if empty
    :return: true if the column is empty, false if otherwise
    """
    s_temp = df[col].replace("", np.nan)
    return s_temp.isnull().all()


def clean_text_cols(df, col):
    """remove leading white space in coloumn.
    remove trailing white space in coloumn.
    remove double space in coloumn.

    :param df: the dataframe

    :param col: the column to clean

    :return: the clean dataframe
    """
    df[col] = df[col].apply(str)
    # remove leading white space
    df[col] = df[col].str.lstrip()
    # remove trailing white space
    df[col] = df[col].str.rstrip()
    df[col] = df[col].str.strip()

    # remove double space
    df[col] = df[col].apply(lambda x: " ".join(x.split()))

    # remove space comma space
    df[col] = df[col].str.replace(" , ", ", ")

    df[col] = df[col].str.rstrip(";")
    df[col] = df[col].str.rstrip(",")

    df[col] = df[col].str.lstrip(";")
    df[col] = df[col].str.rstrip(",")

    df[col] = df[col].str.replace("''", '"')
    df[col] = df[col].str.lstrip(",")
    df[col] = df[col].str.lstrip(":")

    # remove leading white space
    df[col] = df[col].str.lstrip()

    df[col] = df[col].str.strip("\n")

    return df


def drop_col_if_exists(df, col):
    """
    Drop Column if exists
    :param df: the original dataframe
    :param col: the column to drop from dataframe
    :return: the new dataframe
    """
    if col in list(df.columns.values):
        return df.drop(col, axis=1)
    else:
        return df


def rstrip_semicolon(df, col):
    """
    strip semicolumn from end of string in a column

    col -
    :param df: the dataframe to work on
    :param col: the name of the column to work on
    :return: the clean dataframe
    """
    df[col] = df[col].apply(str)
    df[col] = df[col].str.rstrip(";")
    return df


def strip_whitespace_af_semicolon(df, col):
    """
    strip whitespace after semicolon

    :param df: df - the dataframe to work on
    :param col: col - the name of the column to work on
    :return: the clean dataframe
    """
    df[col] = df[col].apply(str)
    from VC_collections.value import semiColonStriper

    df[col] = df[col].apply(semiColonStriper)
    df[col] = df[col].str.replace("; ", ";")
    return df


def remove_line_breaks(df):
    # replace all line breaks
    df = df.replace("\n", " ", regex=True)
    df = df.replace(",,", ",", regex=False)
    df = df.replace(", ,", ", ", regex=False)
    return df


def replace_NaN(df):
    return df.replace(np.nan, "", regex=True)


def clean_tables(collection):
    # replace all NaN values with empty string
    logger = logging.getLogger(__name__)
    logger.info(
        f"Replacing all NaN values with empty string in {collection.collection_id} Catalog records,"
        f"applying {replace_NaN.__name__} function."
    )
    collection.df_catalog = replace_NaN(collection.df_catalog)
    logger.info(
        f"Replacing all NaN values with empty string in {collection.collection_id}, Collection"
        f" record, applying {replace_NaN.__name__} function."
    )
    collection.df_collection = replace_NaN(collection.df_collection)

    # remove line breaks
    logger.info(
        f"Replacing all line breaks in {collection.collection_id}, Catalog records,"
        f"applying {remove_line_breaks.__name__} function."
    )
    collection.df_catalog = remove_line_breaks(collection.df_catalog)
    logger.info(
        f"Replacing all line breaks in {collection.collection_id}, Collection record,"
        f"applying {remove_line_breaks.__name__} function."
    )
    collection.df_collection = remove_line_breaks(collection.df_collection)

    # clean text columns

    for field in field_types_dict["text"]:
        logger.info(
            f"[{field}] Cleaning {field} column - Removing whitespaces (applying  {clean_text_cols.__name__}) "
        )
        if column_exists(collection.df_collection, field) and not is_column_empty(
            collection.df_collection, field
        ):
            collection.df_collection = clean_text_cols(collection.df_collection, field)
        if column_exists(collection.df_catalog, field) and not is_column_empty(
            collection.df_catalog, field
        ):
            collection.df_catalog = clean_text_cols(collection.df_catalog, field)

    # clean values columns - rstrip_semicolon, strip_whitespace_af_semicolon
    for field in field_types_dict["value_list"]:
        logger.info(
            f"[{field}] Cleaning {field} column - Removing whitespaces (applying  {rstrip_semicolon.__name__} "
        )
        if column_exists(collection.df_collection, field) and not is_column_empty(
            collection.df_collection, field
        ):
            collection.df_collection = rstrip_semicolon(collection.df_collection, field)
        if column_exists(collection.df_catalog, field) and not is_column_empty(
            collection.df_catalog, field
        ):
            collection.df_catalog = rstrip_semicolon(collection.df_catalog, field)

        logger.info(
            f"[{field}] Cleaning {field} column - Removing whitespaces (applying  {strip_whitespace_af_semicolon.__name__} "
        )
        if column_exists(collection.df_collection, field) and not is_column_empty(
            collection.df_collection, field
        ):
            collection.df_collection = strip_whitespace_af_semicolon(
                collection.df_collection, field
            )
        if column_exists(collection.df_catalog, field) and not is_column_empty(
            collection.df_catalog, field
        ):
            collection.df_catalog = strip_whitespace_af_semicolon(
                collection.df_catalog, field
            )

    return collection
