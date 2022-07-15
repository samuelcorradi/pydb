from __future__ import annotations
from storagy.conn import Conn as Super
from storagy.utils import Filepath
from storagy.conn.directory import Conn as DirectoryConn
from storagy.filter import Filter
from io import SEEK_END
from os import rename, sep
from os.path import splitext
import os

class Conn(Super):

    def __init__(self
        , path:str
        , filename:str
        , mode:str='r'
        , encode:str='utf-8'
        , default_col_name='col0'):
        self._dir_conn = DirectoryConn(path)
        self._filename = filename
        self._mode = mode
        self._encode = encode
        self._default_col_name = default_col_name
        super().__init__()

    def _connect(self):
        filepath = self._dir_conn._path.append_file(self._filename)
        handler = open(str(filepath), self._mode, encoding=self._encode)
        handler.seek(0, SEEK_END) # go to the file end.
        #if handler.tell()>3:
        #    handler.seek(handler.tell() - 3, SEEK_SET)
        self.__eof = handler.tell() # get the end of file location
        # print(self._filename, self.__eof)
        handler.seek(0, 0) # go back to file beginning
        return handler

    def _disconnect(self):
        self._handler.close()
        self._handler = None

    def _map(self, exp, data):
        m = [True]*len(data)
        field_list = self.field_list()
        parsed = Filter(exp).parse()
        for i, data in enumerate(data):
            row = dict(zip(field_list, data))
            finded = True
            for exp in parsed:
                finded = True
                for j, v in exp.items():
                    if j not in row:
                        continue
                    if row[j] not in v:
                        finded = False
                        break
            m[i] = finded
        return m
        
    def __str__(self)->str:
        return self.get_filepath()
        
    def filter(self, exp):
        """
        Filter data by conditions.
        """
        fdata = []
        data = self.all()
        mapper = self._map(exp, data)
        for i, m in enumerate(mapper):
            if m:
                fdata.append(data[i])
        return fdata

    def field_list(self):
        return [self._default_col_name]

    def insert_list(self, data:list):
        """
        ...
        """
        # TODO: sera que os valores chegaram na mesma
        # ordem que foram passados?
        if type(data) is not list:
            raise Exception("Data is not a list.")
        self._handler.seek(0, SEEK_END)
        self._handler.write(str(data[0]) + os.linesep)
        self.rewind()

    def insert_dict(self, data:dict):
        """
        ...
        """
        if type(data) is not dict:
            raise Exception("The data is not a dictionary.")
        d = data.get(self._default_col_name, None)
        if d is None:
            raise Exception("The data represented by a dictionary should use the key '{}'.".format(self._default_col_name))
        self._handler.seek(0, SEEK_END)
        self._handler.write(str(d) + os.linesep)
        self.rewind()

    def insert(self, data):
        """
        ...
        """
        if type(data) is dict:
            self.insert_dict(data) # if self._has_header else self.insert_list(data.values())
        elif type(data) is list:
            self.insert_list(data)
        else:
            raise Exception("Data needs to be in a list or dictionaty.")

    def get_filepath(self)->str:
        """
        ...
        """
        return os.path.join(str(self._dir_conn), self._filename)

    def get_filename(self)->str:
        return self._filename

    def eof(self)->bool:
        return self._handler.tell()==self.__eof

    def all(self)->list:
        """
        Metodo para carregar o dataset.
        """
        self.rewind()
        r = []
        while True:
            line = self._handler.readline()
            if not line:
                break
            r.append([line.strip()])
        return r

    def check_content(self, ctn:str)->bool:
        """
        Verifica se o arquivo possui
        alguma linha que comece com o 
        conteudo passado.
        """
        finded = False
        self.rewind()
        while not self.eof():
            row = self.get_handler().readline()
            if row.startswith(ctn):
                finded = True
                break
        self.rewind()
        return finded

    # TODO: arrumar isso
    def rename(self, newname:str):
        filepath = self._dir_conn._path.append_file(self._filename)
        path = Filepath(newname)
        s = splitext(path)
        path = s[0]
        filename = s[1]
        if path.endswith(sep):
            return path[:-len(sep)]
        if not filename:
            filename = filepath.get_filename()
        self.disconnect()
        new_path = Filepath(path + "/" + filename)
        rename(filepath, new_path)
        self._filepath = new_path
        self.connect()
        return self

    def is_empty(self):
        """
        Check if the source is empty.
        """
        return os.stat(self.get_filepath()).st_size==0

    def rewind(self):
        self.get_handler().seek(0, 0)
        return self

    def truncate(self):
        self.erase()
        return self

    def erase(self):
        """
        Remove all records.
        """
        self.disconnect()
        self._mode = 'w+'
        self.connect()
        return self

    def select(self, cols:list=[], where=None, limit=None):
        """
        Select data.
        """
        _field_list = self.field_list()
        _cols = [_field_list.index(c) if type(c) is str else int(c) for c in cols]
        data = self.all()
        if not limit: limit = len(data)
        mapper = self._map(where, data)
        res_list = []
        for i, v in enumerate(mapper):
            if i>=limit:
                break
            if v:
                res_list.append([data[i][j] for j in _cols] if _cols else data[i])
        # res_list = [[data[i][j] for j in _cols]  for i, v in enumerate(mapper) if v] 
        return res_list
