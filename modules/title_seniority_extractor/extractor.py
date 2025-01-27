import re
from abc import ABC, abstractmethod
class Extractor(ABC):
    @abstractmethod
    def extract(tite:str, description:str)-> tuple[str, str]:
        raise NotImplementedError
    

class Regex_extractor(Extractor):
    def extract(self, title: str, description: str) -> tuple[str, str]:
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
                "Junior": r"(?i)\b(junior|jr|graduate|entry|fresh|trainee|level 1|l1|grade 1|I)\b",
                "Mid-level": r"(?i)\b(mid|intermediate|level 2|l2|grade 2|II)\b",
                "Senior": r"(?i)\b(senior|sr|lead|principal|staff|architect|level 3|l3|grade 3|III)\b",
            },
            "description": {
                "Junior": r"(?i)\b([0-2])\+?\s*years?(.*experience)?\b",
                "Mid-level": r"(?i)\b([2-4])\+?\s*years?(.*experience)?\b",
                "Senior": r"(?i)\b([5-9]|10)\+?\s*years?(.*experience)?\b",
            }
        }

        # Default values
        job_title = "Not Specified"
        seniority = "Not Specified"

        # Check job roles from title
        for role, pattern in role_patterns.items():
            if re.search(pattern, title):
                job_title = role
                break

        # Check seniority from title
        for level, pattern in seniority_patterns["title"].items():
            if re.search(pattern, title):
                seniority = level
                break

        # Check seniority from description if not found in title
        if seniority == "Not Specified":
            for level, pattern in seniority_patterns["description"].items():
                if re.search(pattern, description):
                    seniority = level
                    break

        return job_title, seniority