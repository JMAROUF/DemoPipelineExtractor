import os
from operator import truediv
from typing import List, Union

from src.utils.constants import file_mode


class ConfigHandler():
    def __init__(self):
        pass

    def _check_if_folder_local(self,path)->bool:
        if os.path.isdir(path):
            return True

    def _list_dir_files(self,path:str)->Union[str,List[str]]:
        files = []
        if self._check_if_folder_local(path):
            for file in os.listdir(path):
                if os.path.isfile( os.path.join(path,file)):
                    files.extend(file if str(file).endswith(file_mode) else [])
            return files
        return path if str(path).endswith(file_mode) else ""

    def _read_as_string_local(self,path:str,encoding:str="utf-8")->List[str]:
        if not self._check_if_folder_local(path):
            return [open(path,encoding=encoding).read()]
        return [open(os.path.join(path,file),encoding=encoding).read()  \
                for file in self._list_dir_files(path)]
