"""
SYNOPSIS
    TODO helloworld [-h,--help] [-v,--verbose] [--version]

DESCRIPTION
    TODO This describes how to use this script. This docstring
    will be printed by the script if there is an error or
    if the user requests help (-h or --help).
    
PROJECT NAME:
    untitled

AUTHOR
    Yael Vardina Gherman <Yael.VardinaGherman@nli.org.il>
    Yael Vardina Gherman <gh.gherman@gmail.com>

LICENSE
    This script is in the public domain, free from copyrights or restrictions.

VERSION
    Date: 29/07/2019 15:22
    
    $
"""

import pandas as pd

from VC_collections.authorities import find_name, find_role
from VC_collections.columns import drop_col_if_exists


def splitDataFrameList(df, target_column, separator):
    """
    :param df: dataframe to split
    :param target_column: the column containing the values to split
    :param separator: the delimiter used to perform the split
    :return: a dataframe with each entry for the target column separated, with each element moved into a new row.
    The values in the other columns are duplicated across the newly divided rows.
    before applying the function the index of the dataframe needs to be reset.
    """

    def splitListToRows(row, row_accumulator, target_column, separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)

    new_rows = []
    df.apply(splitListToRows, axis=1, args=(new_rows, target_column, separator))
    new_df = pd.DataFrame(new_rows)
    return new_df


def explode_col_to_new_df(df, col, sep=";", start=0):
    """
     Explode column into new dataframe

    :param df: the dataframe
    :param col: the column with the values to explode.
    :param sep: the delimiter to split on. Semicolon as default
    :param start:
    :return:
    """
    df_explode = (
        df[col]
        .str.split(sep, expand=True)
        .rename(columns=lambda x: col + f"_{start + x + 1}")
    )
    df = pd.concat([df, df_explode], axis=1)
    df = df.fillna("")
    df = drop_col_if_exists(df, col)
    return df


def horizontal_explode(df_temp, col, col_name, separator=";"):
    """
    Horizontal explode + Col name

    :param df_temp: the original dataframe
    :param col: the column as series
    :param col_name: the column name
    :param separator: the delimiter on which to explode
    :return: the exploded dataframe
    """
    if col in df_temp.columns.values:

        for i, row in df_temp.iterrows():
            count = 1
            for lang in str(row[col]).split(separator):
                colLANGUAGE = col_name + "{}".format(count)
                df_temp.loc[i, colLANGUAGE] = lang.strip()

                count += 1
    return df_temp


def horizontal_explode_creators(df_temp):
    """
    Horizontal explode Creators

    :param df_temp: the original dataframe
    :return: the exploded creators dataframe
    """
    if (
        "COMBINED_CREATORS_PERS" in df_temp.columns.values
        and "COMBINED_CREATORS_CORPS" in df_temp.columns.values
    ):

        for i, row in df_temp.iterrows():
            count = 1
            for name in str(row["COMBINED_CREATORS_PERS"]).split(";"):
                colnamePERS = "CREATOR_PERS{}".format(count)
                colnamePERSROLE = "CREATOR_PERS_ROLE{}".format(count)

                df_temp.loc[i, colnamePERS] = find_name(name).strip()
                df_temp.loc[i, colnamePERSROLE] = find_role(name).strip()

                count += 1

            count = 1
            for name in str(row["COMBINED_CREATORS_CORPS"]).split(";"):
                colnameCORPS = "CREATOR_CORPS{}".format(count)
                colnameCORPSROLE = "CREATOR_CORPS_ROLE{}".format(count)
                df_temp.loc[i, colnameCORPS] = find_name(name).strip()
                df_temp.loc[i, colnameCORPSROLE] = find_role(name).strip()
                count += 1
    else:
        for i, row in df_temp.iterrows():
            count = 1
            for name in str(row["CREATOR_PERS"]).split(";"):
                colnamePERS = "CREATOR_PERS{}".format(count)
                colnamePERSROLE = "CREATOR_PERS_ROLE{}".format(count)

                df_temp.loc[i, colnamePERS] = find_name(name)
                df_temp.loc[i, colnamePERSROLE] = find_role(name)

                count += 1
            count = 1
            for name in str(row["CREATOR_CORP"]).split(";"):
                colnameCORPS = "CREATOR_CORPS{}".format(count)
                colnameCORPSROLE = "CREATOR_CORPS_ROLE{}".format(count)
                df_temp.loc[i, colnameCORPS] = find_name(name)
                df_temp.loc[i, colnameCORPSROLE] = find_role(name)
                count += 1

    return df_temp
