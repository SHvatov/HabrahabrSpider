import os
from collections import Counter
from typing import Tuple, AnyStr, Set

from person_extractor import Personality, extract_personalities_from_document
from term_extractor import extract_terms_from_documents

dirname = os.path.dirname(__file__)
PATH_TO_TXT_DIR = os.path.join(dirname, '../data/habr/txt')


def process_file(path: str) -> Tuple[AnyStr, Set[Personality]]:
    print(f"Processing file: {path}\n")
    with open(path, "r", encoding="UTF-8") as file:
        text = " ".join(file.readlines())
        return text, extract_personalities_from_document(text)


# noinspection PyUnresolvedReferences
def process_dir() -> None:
    print(f"Started processing files in the following dir: {PATH_TO_TXT_DIR}")

    personalities = list()
    processed_files = list()
    for file in os.scandir(PATH_TO_TXT_DIR):
        if file.is_file() and file.path.endswith(".txt"):
            t, p = process_file(file.path)
            processed_files.append(t)
            personalities.extend(p)
            # break  # process only first for testing

    terms = extract_terms_from_documents(processed_files)
    print(f"Processed all files in directory: {PATH_TO_TXT_DIR}. Total files: {len(processed_files)}")
    print(f"Total personalities extracted: {len(personalities)}")
    print(f"Total tokens extracted: {len(terms)}")

    print(sorted(terms, reverse=True, key=lambda _: _.average_tfidf)[0:30])

    counter = Counter(personalities)
    print(f"Top-10 Extracted personalities: {counter.most_common(10)}")

    # required to build word cloud
    for p, c in counter.items():
        print(f'{p.fullname};{c}')


if __name__ == '__main__':
    process_dir()
