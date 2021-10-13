import dataclasses
from typing import List, Set

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from natasha_adapter import DocumentBuilder, MORPH_VOCAB, STOP_WORDS


@dataclasses.dataclass(frozen=True)
class TermData:
    term: str
    average_tfidf: float


def preprocess_token(token: str) -> str:
    modified_token = token.lower()
    # if 'токен' in modified_token:
    #     modified_token = modified_token[:modified_token.find('токен')] + 'токен'
    #
    # if 'блокчейн' in modified_token:
    #     modified_token = modified_token[:modified_token.find('блокчейн')] + 'блокчейн'

    return modified_token


def extract_terms_from_documents(texts: List[str]) -> Set[TermData]:
    preprocessed_texts = list()
    for text in texts:
        document = DocumentBuilder() \
            .text(text) \
            .include_name_extractor() \
            .include_syntax_analyser() \
            .include_morph_analyser() \
            .build()

        for token in document.tokens:
            token.lemmatize(MORPH_VOCAB)

        tokens = [_.lemma for _ in document.tokens if _.pos != 'PUNCT' and _.lemma not in STOP_WORDS]
        preprocessed_texts.append(' '.join([preprocess_token(t) for t in tokens]))

    vectorizer = TfidfVectorizer()
    texts_tfidf_matrix = vectorizer.fit_transform(preprocessed_texts)

    # id2word = {index: row for index, row in enumerate(vectorizer.get_feature_names_out())}
    df = pd.DataFrame(texts_tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())

    term_data = set()
    for token, tfidf in df.mean(axis=0).iteritems():
        term_data.add(
            TermData(token, tfidf)
        )

    return term_data
