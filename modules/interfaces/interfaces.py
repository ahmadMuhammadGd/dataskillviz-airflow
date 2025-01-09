import json, os
from abc import ABC, abstractmethod
from typing import Any

class User_interface(ABC):
    @abstractmethod
    def read(path:str)->dict[str, Any]|Any:
        raise NotImplementedError
    

class Json_interface(User_interface):
    def read(self, path:str)->dict[str, Any]|Any:
        with open(path, 'r') as f:
            return json.load(f)

class Sql_interface(User_interface):
    def read(self, path:str)->str:
        with open(path, 'r') as f:
            return f.read()

    def reads(self, path:str)->list[str]:
        import glob
        files = glob.glob(f"{path}/*.sql")
        files = sorted(files, key=os.path.basename)
        return [self.read(file) for file in files]
