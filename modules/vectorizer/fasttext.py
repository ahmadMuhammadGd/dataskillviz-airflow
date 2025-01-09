from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import re
import random
from tqdm import tqdm
from typing import Dict, Set, List, Optional
from collections import defaultdict
from itertools import chain
import compress_fasttext
import numpy as np
from abc import ABC, abstractmethod
import requests
import json

class SemanticPreprocessor:
    def __init__(self, bigrams:list[str], aliases:dict[str, str]):
        try:
            self.lemmatizer = WordNetLemmatizer()
            self.stop_words = set(stopwords.words('english'))
        except:
            import nltk
            nltk.download('wordnet')
            nltk.download('stopwords')
            self.lemmatizer = WordNetLemmatizer()
            self.stop_words = set(stopwords.words('english'))

        self.bigrams = self._process_bigrams(self._load_raw(bigrams))
        self.aliases = self._process_aliases(self._load_raw(aliases))
        self.ngrams = [*self.bigrams] + list(set(filter(lambda x: '_' in x , chain(*self.aliases.values()))))
        self.unigrams_to_normalize = {'python'}

        self.reverse_aliases = defaultdict(set)
        for main, variations in self.aliases.items():
            for var in variations:
                self.reverse_aliases[var].add(main)

    def _load_raw(self,text:str) -> list[any]|dict[any]|None:
        response = requests.get(text)
        if response.status_code == 200:
            return json.loads(response.content.decode('utf-8'))
        else:
            return None
        
    def _process_bigrams(self, bigrams:list[str])->Set[str]:
        bigrams = {re.sub("\s", " ", bigram).strip().lower() for bigram in bigrams}
        return bigrams

    def _process_aliases(self, aliases:dict[str,str])->dict[str, str]:
        aliases = {re.sub("\s", " ", key).strip().lower(): [re.sub("\s", " ", alias).strip().lower().replace(" ", "_") for alias in values] for key, values in aliases.items()}
        return aliases

    def _handle_special_chars(self, text: str, forward:bool=True) -> str:
        special_chars_map = {
            'c++': 'cplusplus',
            'c#': 'csharp',
            '.net': 'dotnet',
            'EC2': 'ectwo',
            's3': 'sthree',
            'Route 53': 'routefivethree'
        }

        if forward:
            for original, replacement in special_chars_map.items():
                pattern = r"\b" + re.escape(original.lower()) + r"\b"
                text = re.sub(pattern, replacement.lower(), text)
        else:
            for original, replacement in special_chars_map.items():
                pattern = r"\b" + replacement.lower() + r"\b"
                text = re.sub(pattern, original.lower(), text)
        return text

    def _enforce_semantic_consistency(self, text: str) -> str:
        for alias, variations in self.aliases.items():
            name_variations_regex = "\b" + f"{'|'.join(variations)}" + "\b"
            matches = set(re.findall(name_variations_regex, text))
            for m in matches:
                text = text.replace(m, f"{alias} {m}")

        return text

    def preprocess(self, text: str) -> str:
        text = text.lower()
        text = self._handle_special_chars(text, forward=True)  # encode important words with special characters
        text = re.sub(r'[^a-z\s]+', ' ', text)
        text = self._handle_special_chars(text, forward=False) # decode important words with special characters

        for ngram in {*self.ngrams, *self.unigrams_to_normalize}:
            pattern = r"\w*" + ngram.replace("_", r"\s*") + r"\w*"
            text = re.sub(pattern, ngram, text)

        # text = self._enforce_semantic_consistency(text)

        tokens = [word for word in text.split() if word and word not in self.stop_words]
        text = ' '.join([self.lemmatizer.lemmatize(word) for word in tokens])

        return text

    def process_corpus(self, texts: List[str]) -> List[str]:
            return [self.preprocess(text) for text in tqdm(texts, desc="Processing Corpus")]


class Vec(ABC):
    @abstractmethod
    def load(self):
        raise NotImplementedError

    @abstractmethod
    def most_similar(self, text:str, k:int=10)->list[tuple[str, float]]:
        raise NotImplementedError
    
    @abstractmethod
    def get_vector(self, text:str)->np.ndarray:
        raise NotImplementedError


class CustomFasttext(Vec):
    def __init__(self, model_path_or_link:str):
        self.model = None
        self.model_path_or_link = model_path_or_link
        self.load()
                
    def load(self):
        self.model = compress_fasttext.models.CompressedFastTextKeyedVectors.load(self.model_path_or_link)

    def most_similar(self, text:str, k:int=10)->list[tuple[str, float]]:
        return self.model.most_similar(positive=[text], topn=k)
    
    def get_vector(self, text:str)->np.ndarray:
        return self.model.get_vector(text)
