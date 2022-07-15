from storagy.conn import Conn as Super
from storagy.filter import Filter
from storagy.conn.flatfile import Conn as FlatFileConn
import csv
import os

class Conn(Super):

    def __init__(self
        , path:str
        , filename:str
        , mode='r'
        , encode='utf-8'
        , has_header=True
        , delimiter=','
        , quote='"'
        , default_col_name='col{}'
        , field_list:list=[]):
        """
        ...
        """
        self._path = path
        self._filename = filename
        self._mode = mode
        self._encode = encode
        self._has_header = has_header
        self._delimiter = delimiter
        self._quote = quote
        self._default_col_name = default_col_name
        super().__init__()
        # validates the file against the fields
        if self.is_empty() and self._has_header:
            if not field_list:
                raise Exception("O objeto CSVConn indica que o ficheiro CSV possui cabeçalho, mas o ficheiro está vazio. Neste caso é preciso informar as ")
            w = csv.writer(self._handler.get_handler(), delimiter=self._delimiter, quotechar=self._quote)
            w.writerow(field_list)

    def _connect(self):
        flat_conn = FlatFileConn(path=self._path
            , filename=self._filename
            , mode=self._mode
            , encode=self._encode)
        return flat_conn

    def _disconnect(self):
        self._handler.disconnect()
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
    
    def _list_to_dict(self, data:list):
        """
        TODO: criar um metodo para tratar os dados
        a serem inseridos.
        """
        if type(data) is not list:
            raise Exception("Variable 'data' is not a list. Only lists can be converted to a dictionary.")
        flist = self.field_list()
        to_insert = {}
        for i, colval in enumerate(data):
            to_insert[flist[i]] = colval 
        return to_insert

    def _check_dict_keys(self, data_dict:dict, allowed_keys:list):
        """
        Dictionaries are used to reference
        rows of data. If any dictionary
        key is not part of the field list,
        it throws an error.
        """
        # check if the keys are fields in field list
        wrong_keys = []
        keys = data_dict.keys()
        for k in keys:
            if k not in allowed_keys:
                wrong_keys.append(k)
        return wrong_keys

    def __str__(self)->str:
        return self.get_filepath()

    def select(self
        , cols:list=[]
        , where=None,
        limit=None)->list:
        """
        Select data.
        """
        _field_list = self.field_list()
        _cols = [_field_list.index(c) if type(c) is str else int(c) for c in cols]
        data = self.all()
        if not limit: limit = len(data)
        mapper = self._map(where, data)
        r = []
        for i, v in enumerate(mapper):
            if i>=limit:
                break
            if v:
                r.append([data[i][j] for j in _cols] if _cols else data[i])
        # res_list = [[data[i][j] for j in _cols]  for i, v in enumerate(mapper) if v] 
        return r

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

    def eof(self)->bool:
        """
        ...
        """
        return self._handler.eof()

    def get_filename(self)->str:
        """
        ...
        """
        return self._handler.get_filename()

    def get_filepath(self)->str:
        """
        ...
        """
        return self._handler.get_filepath()

    def field_list(self):
        """
        If the CSV has header, return the
        first row. Otherwise create cols
        name using "_default_col_name" and
        the number of columns in the CSV.
        """
        self._handler.rewind()
        if self._has_header:
            if self.is_empty():
                raise Exception("Não é possível encontrar definições de campos e colunas se o ficheiro está vazio. Atualize a definicao dos campos antes de começar a utilizar um CSV que possua cabeçalhos.")
            reader = csv.reader(self._handler.get_handler(), delimiter=self._delimiter, quotechar=self._quote)
            row = next(reader)
            self._handler.rewind()
            return row
        else:
            return [self._default_col_name.format(i+1) for i in range(len(row))]

    def is_empty(self):
        """
        Check if the source is empty.
        """
        return os.stat(self.get_filepath()).st_size==0

    def insert_list(self, data:list):
        """
        ...
        """
        # TODO: sera que os valores chegaram na mesma
        # ordem que foram passados?
        if type(data) is not list:
            raise Exception("Data is not a list.")
        w = csv.writer(self._handler.get_handler(), delimiter=self._delimiter, quotechar=self._quote)
        w.writerow(data)

    def insert_dict(self, data:dict):
        """
        ...
        """
        if type(data) is not dict:
            raise Exception("The data is not a dictionary.")
        header = self.field_list()
        wrong_keys = self._check_dict_keys(data_dict=data, allowed_keys=header)
        if wrong_keys:
            raise Exception("The key {} doesn't exists on the field list.".format(wrong_keys[0]))
        w = csv.DictWriter(self._handler.get_handler(), fieldnames=header, delimiter=self._delimiter, quotechar=self._quote)
        w.writerow(data)

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

    def bulk_insert(self, data:list):
        """
        ...
        """
        if data:
            # if CSV has header but the file is empty, write the fields in the first line
            if self._has_header and self.is_empty():
                self.insert_list(self.field_list())
            rtype = type(data[0])
            if rtype is dict:
                writer = csv.DictWriter(self._handler.get_handler()
                    , fieldnames=self.field_list()
                    , delimiter=self._delimiter
                    , quotechar=self._quote)
            elif rtype is list:
                writer = csv.writer(self._handler.get_handler()
                    , delimiter=self._delimiter
                    , quotechar=self._quote)
            else:
                raise Exception("Data needs to be in a list or dictionaty.")
            # insert all data usign the right writer
            for d in data:
                writer.writerow(d)

    def all(self)->list:
        """
        Return all rows in the CSV.
        If the CSV has header, the
        header info is excluded from
        results.
        """
        all = []
        # retorns the file cursor to the first position
        self._handler.rewind()
        reader = csv.reader(self._handler.get_handler(), delimiter=self._delimiter, quotechar=self._quote)
        if not self.is_empty():
            # if the file has header, jump the first row
            if self._has_header:
                next(reader)
            for row in reader:
                all.append(row)
        return all

    def get_handler(self):
        return self._handler

    def truncate(self):
        return self.erase()

    def erase(self):
        """
        Remove all records.
        """
        header = self.field_list()
        self._handler.erase()
        if header and self._has_header and self.is_empty():
            w = csv.writer(self._handler.get_handler(), delimiter=self._delimiter, quotechar=self._quote)
            w.writerow(header)
        return self
