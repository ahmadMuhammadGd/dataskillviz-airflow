import requests
from modules.text_cleaner.cleaner import TextCleaner
from abc import ABC, abstractmethod
import logging 

class TagExtractor(ABC):
    @abstractmethod
    def extract()->list[str]:
        raise NotImplementedError
    
class DictionaryBasedTagExtractor(TagExtractor):
    def __init__(self, reverse_dict_uri:str):
        self.reverse_dict_uri   =   reverse_dict_uri
        self.reverse_dict       =   None
        self.text_cleaner       =   TextCleaner()
    
        self.__init_reverse_dict()
        self.ngram_min, self.ngram_max = self._get_min_max_ngram()

    def __init_reverse_dict(self):
        response = requests.get(self.reverse_dict_uri)
        if response.status_code == 200:
            self.reverse_dict = response.json()
            
            logging.debug(f'reverse dict: {self.reverse_dict} ..')
            if not isinstance(self.reverse_dict, dict):
                raise Exception(f"expected dict, check reverse dict url and try again")
        else:
            raise Exception(f"An error occured while ingesting `reverse dict`, status code: {response.status_code}")
         
    def _get_min_max_ngram(self) -> tuple[int, int]:
        keys = self.reverse_dict.keys()
        return min(len(x.split()) for x in keys), max(len(x.split()) for x in keys)

    def extract(self, text: str) -> list[str]:
        def _get_text_ngrams(text: str, min_n: int, max_n: int) -> list[str]:
            tokens = text.split()
            return [
                ' '.join(tokens[i:i+window_size])
                for window_size in range(min_n, max_n + 1)
                for i in range(len(tokens) - window_size + 1)
            ]

        def _extract_with_reverse_dict(cleaned_text: str) -> list[str]:
            ngrams = _get_text_ngrams(cleaned_text, self.ngram_min, self.ngram_max)
            keywords = {
                self.reverse_dict.get(ngram) or self.reverse_dict.get(ngram.replace(' ', ''))
                for ngram in ngrams
            }
            return [keyword for keyword in keywords if keyword]

        cleaned_text = self.text_cleaner.clean_text(text)
        return _extract_with_reverse_dict(cleaned_text)