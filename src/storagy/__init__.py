from __future__ import annotations
import os
from storagy.conn import sqlserver
from storagy.conn import flatfile
from storagy.conn import excel
from storagy.conn import csv
from storagy.conn import directory

class Storagy(object):
    """
    """

    @staticmethod
    def factory(driver:str
        , param:dict):
        """
        """
        localizers = {"sqlserver": sqlserver.Conn
            , "flatfile": flatfile.Conn
            , "excel": excel.Conn
            , "csv": csv.Conn
            , "dir": directory.Conn
        }
        if driver not in localizers:
            raise Exception("Não é possível instanciar um objeto que não existe na lista de Factory.")
        return localizers[driver](**param)

    def __init__(self, driver:str, param:dict):
        """
        """
        self._param = param
        self._driver = Storagy.factory(driver, param)
        self._driver.open()

    def __del__(self):
        """
        """
        if hasattr(self, '_driver'):
            self._driver.close()

    def all(self):
        """
        """
        return self._driver.all()

    def field_list(self):
        """
        """
        return self._driver.field_list()

    def is_empty(self):
        """
        """
        return self._driver.is_empty()

    def insert(self, data):
        """
        """
        return self._driver.insert(data)

    def update(self, data, filter):
        """
        """
        return self._driver.update(data, filter)

    def delete(self, data, filter):
        """
        """
        return self._driver.delete(data, filter)

    def truncate(self, confirm:bool=False):
        """
        Alias for erase.
        """
        return self.erase(confirm)

    def erase(self, confirm:bool=False):
        """
        Remove all records.
        """
        if confirm:
            self._driver.erase()
        return self
        
    def select(self, cols:list=[], where=None, limit=None):
        """
        ...
        """
        _field_list = self.field_list()
        _cols = []
        for c in cols:
            if type(c) is str:
                if c not in _field_list:
                    continue
                _cols.append(_field_list.index(c))
            elif type(c) is int:
                _cols.append(c)
        # _cols = [_field_list.index(c) if type(c) is str else int(c) for c in cols if (type(c) is str and c in _field_list) or type(c) is int ]
        return self._driver.select(cols=_cols, where=where, limit=limit)

    def bulk_insert(self, data):
        """
        """
        return self._driver.bulk_insert(data)

    def filter(self, exp):
        """
        """
        return self._driver.filter(exp)

    def __str__(self) -> str:
        """
        """
        header = self.field_list()
        out = ' | '.join(header) + os.linesep
        out += len(out) * '-' + os.linesep
        for data in self.all():
            out += ' | '.join(data) + os.linesep
        return out


