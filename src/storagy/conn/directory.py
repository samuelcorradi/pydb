from storagy.conn import Conn as Super
from storagy.utils import Filepath
from os.path import isfile, join, isdir
from os import listdir

class Conn(Super):

    @staticmethod
    def sources(path:str, filter:str=None)->list:
        return [f for f in listdir(path) if isfile(join(path, f)) and (not filter or f.startswith(filter))]

    def __init__(self, path:str):
        self._path = Filepath(path)
        super().__init__()
        
    def __str__(self)->str:
        return str(self._path)

    def _connect(self):
        if isdir(self._path) is False:
            raise Exception("Caminho {} nao existe.".format(self._path))
        return True

    def _disconnect(self):
        self._handler = None

    def select(self, filter:str=None)->list:
        return Conn.sources(self._path, filter=filter)
