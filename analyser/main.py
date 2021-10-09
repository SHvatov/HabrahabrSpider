import os
from collections import Counter
from typing import Set

from person_extractor import extract_personalities, Personality

PATH_TO_TXT_DIR = r"D:\projects\scrapy-habr-parser\data\txt"


def process_text(text: str) -> Set[Personality]:
    return extract_personalities(text)


def process_file(path: str) -> Set[Personality]:
    print(f"Processing file: {path}\n")
    with open(path, "r", encoding="UTF-8") as file:
        return process_text(" ".join(file.readlines()))


# noinspection PyUnresolvedReferences
def process_dir() -> None:
    print(f"Started processing files in the following dir: {PATH_TO_TXT_DIR}")

    personalities = list()
    processed_files = 0
    for file in os.scandir(PATH_TO_TXT_DIR):
        if file.is_file() and file.path.endswith(".txt"):
            personalities.extend(process_file(file.path))
            processed_files += 1

    print(f"Processed all files in directory: {PATH_TO_TXT_DIR}. Total files: {processed_files}")

    counter = Counter(personalities)
    print(f"Extracted personalities: {counter.most_common(30)}")

    # required to build word cloud
    print(', '.join([_.fullname for _ in personalities]))


if __name__ == '__main__':
    process_dir()
