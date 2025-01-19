import re
import nltk
from random import choices

class TextCleaner:
    def __init__(self,):
        self.version_regex = r'\d\.+(\d|x)*\.*(\d|x)*|\d{4}|(beta|alpha)\d{1,}|v\d{1,}'
        self.allowed_words = {
            '.net', 'c#', 'c++', 's3', '3d', '1d', '2d', 'p5', 'h2', 'd3', 'cv2', 'h2db', 'e2e'
        }
        self.not_allowed_regex = r'[^a-z\s]'
        try:
            self.stopwords = set(nltk.corpus.stopwords.words('english'))
        except:
            nltk.download('stopwords')
            self.stopwords = set(nltk.corpus.stopwords.words('english'))

        self.add_stopwords(['apache'])
    
    def add_stopwords(self, stopwords: list[str]) -> None:
        self.stopwords.update(stopwords)

    def clean_text(self, text: str) -> str:
        if not isinstance(text, str):
            raise ValueError(f"Input must be a string, got: {type(text)}.")

        allowed_dict = {word: ''.join(choices('abcdefghijklmnopqrstuvwxyz', k=20)) for word in self.allowed_words}

        def _allow_words(t: str, mode: str) -> str:
            for word, code in allowed_dict.items():
                pattern, replacement = (rf'\b{re.escape(word)}', code) if mode == 'encode' else (rf'{re.escape(code)}', word)
                t = re.sub(pattern, replacement, t)
            return t

        t = text.lower()
        t = ' '.join(word for word in t.split() if word not in self.stopwords)
        t = _allow_words(t, mode='encode')
        t = re.sub(self.version_regex, ' ', t).strip()
        t = re.sub(self.not_allowed_regex, ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        t = _allow_words(t, mode='decode')
        return t