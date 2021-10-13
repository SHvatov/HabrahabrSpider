from natasha import (
    Segmenter,
    NewsEmbedding,

    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,

    MorphVocab,
    NamesExtractor,
    Doc
)

from nltk.corpus import stopwords

EMB = NewsEmbedding()
SEGMENTER = Segmenter()

NER_TAGGER = NewsNERTagger(EMB)
MORPH_TAGGER = NewsMorphTagger(EMB)
SYNTAX_PARSER = NewsSyntaxParser(EMB)

MORPH_VOCAB = MorphVocab()
NAMES_EXTRACTOR = NamesExtractor(MORPH_VOCAB)


class DocumentBuilder:
    """
    Basic builder-pattern implementation, which can be used
    in submodules to create documents with specific abilities,
    like name extraction and etc.
    """

    def __init__(self):
        self.__text = None
        self.__include_name_extractor = False
        self.__include_syntax_analyser = False
        self.__include_morph_analyser = False

    def text(self, text: str):  # -> DocumentBuilder
        self.__text = text
        return self

    def include_name_extractor(self):  # -> DocumentBuilder
        self.__include_name_extractor = True
        return self

    def include_syntax_analyser(self):  # -> DocumentBuilder
        self.__include_syntax_analyser = True
        return self

    def include_morph_analyser(self):  # -> DocumentBuilder
        self.__include_morph_analyser = True
        return self

    def build(self) -> Doc:
        document = Doc(self.__text)
        document.segment(SEGMENTER)

        if self.__include_name_extractor:
            document.tag_ner(NER_TAGGER)

        if self.__include_morph_analyser:
            document.tag_morph(MORPH_TAGGER)

        if self.__include_syntax_analyser:
            document.parse_syntax(SYNTAX_PARSER)

        return document


# noinspection PyUnresolvedReferences
def prepare_stop_words():
    import os
    __PATH_TO_STOP_WORDS_DIR = r"D:\projects\scrapy-habr-parser\analyser\raw_stopwords"

    stop_words = set()
    for file in os.scandir(__PATH_TO_STOP_WORDS_DIR):
        if file.is_file() and file.path.endswith(".txt"):
            with open(file.path, 'r', encoding='UTF-8') as txt_file:
                for word in txt_file.readlines():
                    stop_words.add(word.strip().lower())

    return stop_words.union(stopwords.words('russian'))


STOP_WORDS = prepare_stop_words()
