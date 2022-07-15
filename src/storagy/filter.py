import re 

class Filter(object):

    def __init__(self, exp=None):
        self.set_exp(exp)

    def set_exp(self, exp):
        self._exp = exp 

    def parse(self):
        """
        ..
        AND {'coluna?EQUAL':2, 'col4':52}
        OR () ({'coluna?EQUAL':2}, {'colunaB':3})
        OR [] {'coluna':[2,3]}
        <> {'coluna?DIFF': 5}
        """
        flt = []
        # OR
        if type(self._exp) is tuple:
            for e in self._exp:
                eobj = Filter(e)
                flt += eobj.parse()
        # AND
        elif type(self._exp) is dict:
            r = {}
            pattern = r'^(.+)(?:\!([A-Z]))?'
            for key, value in self._exp.items():
                match = re.findall(pattern=pattern, string=key)
                r[key] = value if type(value) is list else [value]
            flt.append(r)
        elif type(self._exp) is list:
            pass
        return flt

    def as_sql(self):
        """
        Use the parser result to write the
        correspodent WHERE clausule in SQL
        format.
        """
        def parser_value(val):
            if type(val) is str:
                return "'{}'".format(val)
            elif type(value) is list:
                vlist = []
                for v in value:
                    vlist.append(parser_value(v))
                return ', '.join(vlist)
            else:
                return v
        parsed = self.parse()
        print(parsed)
        or_groups = []
        for p in parsed:
            and_group = []
            for field, value in p.items():
                cmd = '' + field
                if type(value) is str:
                    cmd += " = {}".format(parser_value(value))
                elif type(value) is list:
                    cmd += " IN ({})".format(parser_value(value))
                and_group.append(cmd)
            or_groups.append(' AND '.join(and_group))
        query = ' OR '.join(or_groups)
        print(query)
        return query



if __name__=="__main__":
    flt = Filter()
    flt.set_exp(exp=({'a':2, 'b':'LALALA'}, {'a':44}, {'b':'Samu'}))
    print(flt.parse())