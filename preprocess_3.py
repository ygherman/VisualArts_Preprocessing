import json
import sys

sys.path.insert(
    1, "C:/Users/Yaelg/Google Drive/National_Library/Python/VC_Preprocessing"
)

from VC_collections.authorities import *


def extract_dictionary_from_conf(file):
    with open(file, encoding="utf8") as json_file:
        file_data = json.load(json_file)
        print("\n".join([f"{key} :{val}" for key, val in file_data.items()]))
        print("\n")

    return file_data


def search_for_file_with_extention(file_extention: str, directory: Path) -> list:
    """

    :param file_extention:
    :param directory:
    :return: list of file paths with the desired extention
    """
    file_list = list()

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extention):
                print(os.path.join(root, file))

                file_info = extract_dictionary_from_conf(os.path.join(root, file))

                file_list.append(os.path.join(root, file))

    return file_list


def main():
    folder_path = Path().cwd()
    path_to_search = folder_path.parents[1]

    file_list = search_for_file_with_extention("conf", path_to_search)


if __name__ == "__main__":
    main()
