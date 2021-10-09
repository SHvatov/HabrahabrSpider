import dataclasses
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Set, Optional

import wikipedia
from wikipedia import WikipediaException

from natasha_adapter import DocumentBuilder, MORPH_VOCAB, NAMES_EXTRACTOR


class PersonalityType(Enum):
    __order__ = 'INDIVIDUAL COMPANY MEDIA_GROUP'

    def __new__(cls, *args, **kwargs):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    def __init__(self, categories: List[str]):
        self.__categories = categories

    def accepts_category(self, category: str) -> bool:
        return self.__categories.count(category) != 0

    INDIVIDUAL = ["Категория:Персоналии по алфавиту"]
    COMPANY = ["Категория:Компании по алфавиту", "Категория:Сайты по алфавиту"]
    MEDIA_GROUP = ["Категория:Музыкальные коллективы по алфавиту"]


class Personality(ABC):
    @property
    @abstractmethod
    def tokens(self) -> List[str]:
        """
        :return: list of string tokens that form the full name of this personality.
        """
        pass

    @property
    @abstractmethod
    def fullname(self) -> str:
        """
        :return: full name of the personality itself.
        """
        pass

    @property
    @abstractmethod
    def source(self) -> Optional[str]:
        """
        :return: link to the source of the information, that proves that this name is actually
        related to some company or individual, like link to the wiki article.
        """
        pass

    @property
    @abstractmethod
    def type(self) -> PersonalityType:
        """
        :return: the type of the personality - either it is a company or an individual.
        """
        pass

    @property
    @abstractmethod
    def attributes(self) -> Dict:
        """
        :return: additional attributes, that can be retrieved during the processing.
        """
        pass

    def __str__(self) -> str:
        return f"Personality[{self.fullname}, {self.type.name}, {self.source}]"


@dataclasses.dataclass(frozen=True)
class WikiPersonality(Personality):
    __fullname: str
    __wiki_link: str
    __type: PersonalityType

    @property
    def tokens(self) -> List[str]:
        return self.__fullname.split(' ')

    @property
    def fullname(self) -> str:
        return self.__fullname

    @property
    def source(self) -> Optional[str]:
        return self.__wiki_link

    @property
    def type(self) -> PersonalityType:
        return self.__type

    @property
    def attributes(self) -> Dict:
        return dict()


def extract_personalities(text: str) -> Set[Personality]:
    print(f"Started processing text: {text[0:min(100, len(text))]}\n")

    # 1. create document and acquire all the names that are considered to be
    # names of the personalities according to natasha library
    # Note: we still loose some of the very specific usernames, like deadmau5, but it is not very significant
    document = DocumentBuilder() \
        .text(text) \
        .include_name_extractor() \
        .include_syntax_analyser() \
        .include_morph_analyser() \
        .build()

    # 2. At this point we have a list of possible names, that are not normalized. We should normalize them.
    for span in document.spans:
        span.normalize(MORPH_VOCAB)

    # 3. Attempt to extract first and last names, it can be useful when one name appears multiple times
    # due to the minor problems with parsing (like text under images)
    for span in document.spans:
        span.extract_fact(NAMES_EXTRACTOR)

    # 4. Go through all the personalities we do have at the moment
    personalities = set()
    for personality in document.spans:
        # 4.1 if first / last name extraction is successful - use it
        if personality.fact is not None:
            fact_dict = personality.fact.as_dict
            first_name = fact_dict["first"] if "first" in fact_dict.keys() else ""
            last_name = fact_dict["last"] if "last" in fact_dict.keys() else ""
            fullname = f"{first_name} {last_name}".strip()
            personalities.add(fullname)
        # 4.2 if we could not extract first / last name then use normalized version of the name
        else:
            personalities.add(personality.normal)

    print(f"Extracted possible personalities..."
          f"\n\ttotal: {len(personalities)}"
          f"\n\t{', '.join(personalities)}\n")

    # 5. Go to wikipedia and check whether such person or company exists
    wikipedia.set_lang('ru')
    actual_wiki_personalities = set()
    for personality in personalities:
        try:
            print(f"Processing personality: {personality}")

            wiki_personalities = wikipedia.search(personality)
            if len(wiki_personalities) == 0:
                print(f"Nothing found on wiki for the following personality: {personality}\n")
                continue

            wiki_personality = None

            # filter companies
            for temp in wiki_personalities:
                if temp.lower() == f"{personality.lower()} (компания)":
                    wiki_personality = temp
                    break

            # filter social networks
            if wiki_personality is None:
                for temp in wiki_personalities:
                    if temp.lower() == f"{personality.lower()} (социальная сеть)" != 0:
                        wiki_personality = temp
                        break

            if wiki_personality is None:
                wiki_personality = wiki_personalities[0]

            print(f"Found following articles on wiki: {', '.join(wiki_personalities)}")
            print(f"Processing personality according to wiki: {wiki_personality}")

            page = wikipedia.page(wiki_personality)

            personality_type = None
            for category in page.categories:
                for t in PersonalityType:
                    if t.accepts_category(category):
                        personality_type = t
                        break

            if personality_type is not None:
                actual_wiki_personality = WikiPersonality(page.title, page.url, personality_type)
                actual_wiki_personalities.add(actual_wiki_personality)
                print(f"Processed personality: {actual_wiki_personality}\n")
            else:
                print(f"{personality} is unknown to wiki, will be skipped\n")
        except WikipediaException:
            print(f"Exception occurred while processing, will be skipped.\n")
            continue

    print(f"Extracted actual personalities according to Wiki..."
          f"\n\ttotal: {len(actual_wiki_personalities)}\n")
    return actual_wiki_personalities
