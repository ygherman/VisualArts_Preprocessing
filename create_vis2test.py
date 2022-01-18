import sys


def main():
    filenames = list()

    while True:

        filename = str(input("Enter path to file: "))
        if filename == "":
            break
        else:
            filenames.append(filename)

    # file1 = input("path to first file: ")
    # file2 = input("path to second file: ")

    if len(filenames) == 0:
        sys.stderr.write("no files were given")
        sys.exit()

    with open(
        r"vis2test.txt", "w", newline="\n", encoding="utf8", errors="ignore"
    ) as outfile:
        for fname in filenames:
            with open(fname, encoding="utf8") as infile:
                for line in infile:
                    outfile.write(line)


if __name__ == "__main__":
    main()
