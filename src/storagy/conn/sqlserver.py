from storagy.conn import Conn as Super
from storagy.filter import Filter
import pyodbc

class Conn(Super):

    conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s'

    def __init__(self
        , host:str
        , db:str
        , user:str
        , pwd:str
        , tbname:str):
        self.host = host
        self.db = db
        self.user = user
        self.pwd = pwd
        self._tbname = tbname
        self._cursor = None
        super().__init__()

    def _connect(self):
        return pyodbc.connect(Conn.conn_string % (self.host, self.db, self.user, self.pwd))

    def _disconnect(self):
        # self.commit()
        self.close()
        # self._handler.close()
        self._handler = None

    def open(self):
        if self._cursor:
            # self.commit()
            # self.close()
            pass
        self.close()
        self._cursor = self._handler.cursor()

    def commit(self):
        if self._cursor:
            self._cursor.commit()

    def select(self, cols:list=[], where=None, limit=None):
        """
        ...
        """
        _where = ""
        if where:
            flt = Filter(where)
            _where = "WHERE {}".format(flt.as_sql())
        sql_format = "SELECT {limit}{cols} FROM {tbname} {where}"
        limit = "TOP {}".format(limit) if limit else ''
        cols = "*" if not cols else ', '.join(cols)
        query = sql_format.format(limit=limit
            , cols=cols
            , tbname=self._tbname
            , where=_where)
        self.query(query)
        return self._cursor.fetchall()

    def filter(self, exp):
        """
        ...
        """
        _where = ""
        if exp:
            flt = Filter(exp)
            _where = "WHERE {}".format(flt.as_sql())
        sql_format = "SELECT * FROM {tbname} {where}"
        query = sql_format.format(tbname=self._tbname
            , where=_where)
        self.query(query)
        return self._cursor.fetchall()

    def close(self):
        if self._cursor:
            self.commit()
            self._cursor.close()
            self._cursor = None

    def is_empty(self):
        query = "SELECT TOP(1) 1 FROM {tbname}".format(tbname=self._tbname)
        self.query(query)
        r = len(self._cursor.fetchall())==0
        return r
        
    def query(self, sql:str, data:list=None):
        if not self._cursor:
            self.open()
            # raise Exception("Cursor nulo. Eh preciso abrir cursor com metodo open().")
        if data:
            return self._cursor.execute(sql, data)
        return self._cursor.execute(sql)

    def trans_query(self, sql:str, data:list=None):
        self.open()
        self.query(sql, data)
        self.commit()
        self.close()

    def resize(self
        , fieldname:str
        , size:int):
        if not size:
            return
        if size>4000:
            size = 'MAX'
        sql = "ALTER TABLE [{}] ALTER COLUMN [{}] VARCHAR({})".format(self._tbname, fieldname, size)
        return self.trans_query(sql)

    def truncate(self):
        return self.trans_query("TRUNCATE TABLE {};".format(self._tbname))

    def all(self)->list:
        self.query("SELECT * FROM {}".format(self._tbname))
        return self._cursor.fetchall()

    def insert_list(self, data:dict):
        if type(data) is not list:
            raise Exception("Data is not a list.")
        field_list = self.field_list()
        sql = "INSERT INTO dbo.{} ([{}]) VALUES ({})".format(self._tbname
            , '],['.join(field_list[:len(data)])
            , ', '.join(["?"]*len(data)))
        self.query(sql, list(data))
        # print("Inseridos {} registros.".format(len(values)))
        return self

    def insert_dict(self, data:dict):
        if type(data) is not dict:
            raise Exception("The data is not a dictionary.")
        field_list = data.keys()
        values = data.values()
        sql = "INSERT INTO dbo.{} ([{}]) VALUES ({})".format(self._tbname
            , '],['.join(field_list)
            , ', '.join(["?"]*len(field_list)))
        self.query(sql, list(values))
        # print("Inseridos {} registros.".format(len(values)))
        return self

    def insert(self, data:dict):
        """
        ...
        """
        if type(data) is dict:
            self.insert_dict(data) # if self._has_header else self.insert_list(data.values())
        elif type(data) is list:
            self.insert_list(data)
        else:
            raise Exception("Data needs to be in a list or dictionaty.")

    def bulk_insert(self, field_list:list, data:list):
        sql = "INSERT INTO dbo.{} ([{}]) VALUES ({})".format(self._tbname
            , '],['.join(field_list)
            , ', '.join(["?"]*len(field_list)))
        if data:
            self._cursor.executemany(sql, data)
        return self

    def field_list(self):
        self.query("SELECT TOP 1 * FROM {}".format(self._tbname))
        columns = [column[0] for column in self._cursor.description]
        return columns

    def field_size(self)->dict:
        self.open()
        self.query("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name='{}'".format(self._tbname))
        field_size = {}
        for row in self._cursor.fetchall():
            field_size[row[0]] = row[2]
        self.close()
        return field_size

