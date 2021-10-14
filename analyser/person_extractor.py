import dataclasses
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Set

import wikipedia
from wikipedia import WikipediaException

from natasha_adapter import DocumentBuilder, MORPH_VOCAB, NAMES_EXTRACTOR, STOP_WORDS

STOP_WIKI_CATEGORIES = {
    'Категория:Страницы значений по алфавиту',
    'Категория:Государства по алфавиту',
    'Категория:Литературные произведения по алфавиту',
    'Категория:Населённые пункты по алфавиту',
    'Категория:Фильмы по алфавиту',
    'Категория:Блокчейн',
    'Категория:Программное обеспечение по алфавиту',
    'Категория:Криптография',
    'Категория:Криптовалюты',
    'Категория:Музеи по алфавиту',
}


class PersonalityType(Enum):
    __order__ = 'INDIVIDUAL COMPANY MEDIA_GROUP UNVERIFIED'

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
    UNVERIFIED = []


class Personality(ABC):
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

    def __str__(self) -> str:
        return f"Personality[{self.fullname}, {self.type.name}, {self.source}]"


@dataclasses.dataclass(frozen=True)
class WikiPersonality(Personality):
    __fullname: str
    __wiki_link: str
    __type: PersonalityType

    @property
    def fullname(self) -> str:
        return self.__fullname

    @property
    def source(self) -> Optional[str]:
        return self.__wiki_link

    @property
    def type(self) -> PersonalityType:
        return self.__type


@dataclasses.dataclass(frozen=True)
class UnverifiedPersonality(Personality):
    __fullname: str

    @property
    def fullname(self) -> str:
        return self.__fullname

    @property
    def source(self) -> Optional[str]:
        return None

    @property
    def type(self) -> PersonalityType:
        return PersonalityType.UNVERIFIED


def extract_personalities_from_document(text: str) -> Set[Personality]:
    print(f"Started processing text: {text[0:min(100, len(text))]}\n")

    document = DocumentBuilder() \
        .text(text) \
        .include_name_extractor() \
        .include_syntax_analyser() \
        .include_morph_analyser() \
        .build()

    for span in document.spans:
        span.normalize(MORPH_VOCAB)
        span.extract_fact(NAMES_EXTRACTOR)

    personalities = set()
    for personality in document.spans:
        if personality.normal in STOP_WORDS:
            continue
        elif personality.fact is not None:
            fact_dict = personality.fact.as_dict
            first_name = fact_dict["first"] if "first" in fact_dict.keys() else ""
            last_name = fact_dict["last"] if "last" in fact_dict.keys() else ""
            fullname = f"{first_name} {last_name}".strip()
            personalities.add(fullname)
        else:
            personalities.add(personality.normal)

    print(f"Extracted possible personalities..."
          f"\n\ttotal: {len(personalities)}"
          f"\n\t{', '.join(personalities)}\n")

    wikipedia.set_lang('ru')
    actual_personalities = set()
    for personality in personalities:
        try:
            print(f"Processing personality: {personality}")

            wiki_personalities = wikipedia.search(personality)
            if len(wiki_personalities) == 0:
                print(f"Nothing found on wiki for the following personality: {personality}. "
                      f"Will be added as unverified one.\n")
                actual_personalities.add(UnverifiedPersonality(personality))
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
            encountered_stop_category = None
            for category in page.categories:
                for t in PersonalityType:
                    if t.accepts_category(category):
                        personality_type = t
                        break
                if category in STOP_WIKI_CATEGORIES:
                    encountered_stop_category = category
                    break

            if encountered_stop_category is not None:
                print(f"Category {encountered_stop_category} is considered to be a prohibited one, "
                      f"thus this personality will be skipped\n")
                continue

            # if any([str(wiki_personality).lower().count(t.lower()) == 0 for t in personality.split(' ')]):
            #     actual_personalities.add(UnverifiedPersonality(personality))
            #     print("Found wikipedia personality that does not match the actual one, "
            #           "thus it will be added as unverified one.\n")
            #     continue

            if personality_type is not None:
                actual_wiki_personality = WikiPersonality(page.title, page.url, personality_type)
                actual_personalities.add(actual_wiki_personality)
                print(f"Verified personality via wiki: {actual_wiki_personality}\n")
            else:
                print(f"{personality} is unknown to wiki, will be added as unverified one.\n")
                actual_personalities.add(UnverifiedPersonality(personality))
        except WikipediaException:
            print(f"Exception occurred while processing {personality} via wikipedia, "
                  f"thus will be skipped.\n")
            continue

    print(f"Extracted actual personalities according to Wiki..."
          f"\n\ttotal: {len(actual_personalities)}\n")
    return actual_personalities
