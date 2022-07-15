from __future__ import annotations
import os

class Filepath(str):
    """
    """

    def __init__ (self, value):
        self.value = value

    def _replase_bars(self, path:str)->str:
        path = path.replace("\\", "/").replace('/', os.sep)
        return path

    def __str__(self)->str:
        return self._replase_bars(self.value)

    def get_filename(self, remove_ext=False)->str:
        filename = os.path.basename(self.value)
        if remove_ext:
            filename = os.path.splitext(filename)[0]
        return filename

    def add_suffix(self, sufix:str)->Filepath:
        s = os.path.splitext(self.value)
        return Filepath(s[0] + sufix + s[1])

    def get_dir(self):
        s = os.path.splitext(self.value)
        if s[0].endswith(os.sep):
            return s[0][:-len(os.sep)]
        return s[0]

    def append_dir(self, path:str)->Filepath:
        return Filepath(self.get_dir() + "/" + path.strip(os.sep) + '/' + self.get_filename())

    def append_file(self, filename:str)->Filepath:
        return Filepath(self.get_dir() + "/" + filename)