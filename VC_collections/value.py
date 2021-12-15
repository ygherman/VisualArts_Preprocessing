import logging
import re
import sys
from datetime import datetime

import dateutil
import numpy as np
from fuzzywuzzy import process
from langdetect import DetectorFactory, detect
from langdetect.lang_detect_exception import LangDetectException


def filter_characters(character):
    characters_to_filter = [None, np.nan, " "]

    if character in characters_to_filter:
        return True
    else:
        return False


def isNaN(value):
    """
    Checks whether the given value is a NaN.

    :param value: the value to check
    :return: True for NaN and false if not a NaN
    """
    return value != value


def find_nth(string, searchFor, n):
    """
    finds the nth occurrence of a substring (searchFor) in a string.

    :param string: the string to search
    :param searchFor: the substring to search for
    :param n: the position number
    :return: The position of the nth occurrence of the substring in the given string.
    """

    start = string.find(searchFor)
    while start >= 0 and n > 1:
        start = string.find(searchFor, start + len(searchFor))
        n -= 1
    return start


def check_missing_data(df):
    """
    Check Missing Data
    :param df: the dataframe to check
    :return: a sorted list of null values
    """
    # check for any missing data in the df (display in descending order)
    return df.isnull().sum().sort_values(ascending=False)


def utf8len(s):
    """
    checks the length of a given utf8 encoded s string
    :param s: the string to check
    :return: the length of a the given string s
    """
    return len(s.encode("utf-8"))


def create_list(x, delimiter):
    """
        create list from a string with specified delimiter
    :param x: the string to turn to list
    :param delimiter: the delimiter to split the string by
    :return: return the list that was created from the given x string
    """
    x_list = x.split(delimiter)
    x_list = [x.strip() for x in x_list]
    return x_list


def unique_list(text):
    """
        Create a unique list with give string text
    :param text: the given string
    :return: returns a unique list of objects split by semicolon
    """
    text = [x.strip() for x in text.split(";")]
    return ";".join(list(set(text)))


def date_validate(date_text):
    """
        Validate a given Date passed as string type
    :param date_text: the date string to check
    """
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


# strip trailing semicolon function
semiColonStripper = lambda x: x.rstrip(";")

# Cleaning Text from special Characters
clean_text = lambda x: "".join(e for e in str(x) if e.isalnum())

# strip whitespaces function
whiteSpaceStriper = lambda x: x.strip()

clean_name = lambda x: re.sub(r"(?<=[.,])(?=[^\s])", r" ", x)


def clean_date_format(string_date):
    logger = logging.getLogger(__name__)
    string_date_to_datetime = ""
    date_formats_type1 = [
        "%d-%m-%y",
        "%Y-%m-%d",
        "%Y-%d-%m",
        "%Y-%-m-%d",
        "%Y-%m-%-d",
        "%Y-%-m-%-d",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y%m%d %H:%M",
        "%d/%m/%Y %H:%M",
        "%m/%d/%y %H:%M",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d.%m.%y",
    ]

    date_formats_6 = [
        "%Y-%m",
        "%Y-%-m",
        "%m-%Y",
        "%-m-%Y",
        "%m/%Y",
        "%-m/%Y",
        "%Y/%m",
        "%Y/%-m",
    ]

    date_formats_4 = ["%Y"]

    i = 0
    date_to_clean = list()
    string_date_clean = str(string_date).strip()

    if len(string_date_clean) < 5:
        try:
            string_date_to_datetime = datetime.strptime(string_date_clean, "%Y")
            return datetime.strftime(string_date_to_datetime, "%Y")
        except:
            logger.error(
                f"[DATEs - only years] didn't find the right date format for [{string_date_clean}]"
            )
            date_to_clean.append(string_date_clean)

    if 6 <= len(string_date_clean) < 8:
        i = 0
        while True:
            try:
                string_date_to_datetime = datetime.strptime(
                    string_date_clean, date_formats_6[i]
                )
                return datetime.strftime(string_date_to_datetime, "%Y-%m")
            except ValueError:
                i += 1
            except IndexError:
                logger.error(
                    f"[DATEs] didn't find the right date format for [{string_date_clean}]"
                )
                date_to_clean.append(string_date_clean)
                break

    else:
        while True:
            try:
                string_date_to_datetime = datetime.strptime(
                    string_date_clean, date_formats_type1[i]
                )
                break
            except ValueError:
                i += 1
            except IndexError:
                logger.error(
                    f"[DATEs_type1] didn't find the right date format for [{string_date_clean}]"
                )
                date_to_clean.append(string_date_clean)
                break
    if len(date_to_clean) > 0:
        sys.stderr.write(
            f"Please clean these dates: {date_to_clean}\n And re-run application\n\n"
        )
        input("Write this Down!\n")
        return string_date
    return datetime.strftime(string_date_to_datetime, "%Y-%m-%d")


def replace_lst_dict(lst, dictionary):
    for k, v in enumerate(lst):
        if v in dictionary:
            lst[k] = dictionary[v]
    return lst


def format_cat_date(df):
    if clean_text("תאריך הרישום") in list(df.columns):
        cat_date_col = clean_text("תאריך הרישום")
    else:
        cat_date_col = process.extractOne("date_cataloguing", list(df.columns))

    df[cat_date_col] = df[cat_date_col].apply(str)

    df[cat_date_col] = df[cat_date_col].apply(
        lambda x: datetime.strftime(dateutil.parser.parse(x), "%Y%m")
        if len(x) > 6
        else x
    )

    return df


def extract_years_from_text(date_text):
    years = re.findall(r"(\d{4})", str(date_text))
    years = sorted([year for year in years])

    if len(years) < 1:
        return None
    elif len(years) == 1:
        return [years[0].strip(), years[0].strip()]
    else:
        return years


def check_date_values_in_row(date_start, date_end, date_free_text):
    if date_start != "" and date_end != "":
        return date_start, date_end
    elif date_free_text != "":
        date_text = date_free_text
    else:
        sys.stderr.write(f"[DATE] Problem with date columns - please check!")
        sys.exit()

    years = extract_years_from_text(date_text)
    if years is None:
        return None, None
    elif len(years) == 1:
        return years[0], years[0]
    return years[0], years[1]


def is_multi_value(val):
    return ";" in str(val)


def detect_language(text: str):
    DetectorFactory.seed = 0
    try:
        return detect(text)
    except LangDetectException:
        lang_user_input = input(
            f"[Error] Could not detect language! \n Please insert language code manually."
            f"\n Which language is the following text [{text}]? \n "
        )
        return lang_user_input
