import re
from abc import ABC, abstractmethod
class Extractor(ABC):
    @abstractmethod
    def extract(tite:str, description:str)-> tuple[str, str]:
        raise NotImplementedError
    

class Regex_extractor(Extractor):
    def extract(self, title:str, description:str) -> tuple[str, str]:
        """
        returns: (job_title, seniority)
        """
        senior_pattern = {
            "title_roles": r"(?i)\b(senior|sr|lead|principal|staff|architect)\b",
            "level_indicators": r"(?i)\b(level 3|l3|grade 3|III)\b",
            "years_exp": r"(?i)\b([5-9]|10)\+?\s*years?(.*experience)?\b"
        }

        midlevel_pattern = {
            "title_roles": r"(?i)\b(mid|intermediate)\b",
            "level_indicators": r"(?i)\b(level 2|l2|grade 2|II)\b",
            "years_exp": r"(?i)\b([2-4])\+?\s*years?(.*experience)?\b"
        }

        junior_pattern = {
            "title_roles": r"(?i)\b(junior|jr|graduate|entry|fresh|trainee)\b",
            "level_indicators": r"(?i)\b(level 1|l1|grade 1|I)\b",
            "years_exp": r"(?i)\b([0-2])\+?\s*years?(.*experience)?\b"
        }

        role_patterns = {
            "Data Analyst": r"(?i)\b(data.*analy|analyst|analytics|bi.*developer|business.*intelligence|power\s*bi|tableau)\b",
            "Data Engineer": r"(?i)\b(data.*engineer|etl|pipeline)\b",
            # "BI Developer": r"(?i)\b(bi.*developer|business.*intelligence|power\s*bi|tableau)\b",
            "Data Scientist": r"(?i)\b(data\s*scientist)|(machine\s*learning|ml|ai)\s*engineer\b"
        }

        seniority_patterns = {
            "title": {
                "Junior"    : '|'.join([junior_pattern["title_roles"], junior_pattern["level_indicators"]]),
                "Mid-level" : '|'.join([midlevel_pattern["title_roles"], midlevel_pattern["level_indicators"]]),
                "Senior"    : '|'.join([senior_pattern["title_roles"], senior_pattern["level_indicators"]]),
            },
            "description": {
                "Junior"    : junior_pattern["years_exp"],
                "Mid-level" : midlevel_pattern["years_exp"],
                "Senior"    : senior_pattern["years_exp"],
            }
        }

        # Default values
        job_title = "Not Specified"
        seniority = "Not Specified"

        for role, pattern in role_patterns.items():
            if re.search(pattern, title):
                job_title = role
                break

        for level, pattern in seniority_patterns["title"].items():
            if re.search(pattern, title):
                seniority = level
                break

        if seniority == "Not Specified":
            for level, pattern in seniority_patterns["description"].items():
                if re.search(pattern, description):
                    seniority = level
                    break

        return job_title, seniority