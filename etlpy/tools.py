# coding=utf-8
import json
import os
import urllib

from dill import dill
import codecs
CACHE_DB_NAME = '_etlpy_cache.db'
ALLOW_CACHE = True

from etlpy.extends import *
from etlpy.params import request_param, ExpParam
from etlpy.pickledb import pickledb
from etlpy.spider import get_sub_xpath, get_diff_nodes, _get_etree, search_xpath, search_properties, get_datas, \
    get_node_html, get_main, get_node_text
from etlpy.multi_yielder import multi_yield, NORMAL_MODE, NETWORK_MODE, DEFAULT_WORKER_NUM

if PY2:
    pass
else:
    import html

MERGE_APPEND = '+'
MERGE_CROSS = '*'
MERGE_MERGE = '|'
MERGE_MIX = 'mix'


def __get_match_counts(mat):
    return mat.lastindex if mat.lastindex is not None else 1


class ETLTool(EObject):
    '''
    base class for all tool
    '''

    def __init__(self):
        super(ETLTool, self).__init__()
        self.p = ''

    def process(self, data, env):
        return data

    def init(self):
        pass

    def get_p(self, data):
        return query(data, self.p)

    def _is_mode(self, mode):
        if not hasattr(self, 'mode'):
            return False
        return mode in self.mode.split('|')

    def _eval_script(self, p, global_para=None, local_para=None):
        if p == '':
            return True
        if not is_str(p):
            return p(self)
        result = None
        from datetime import datetime
        from time import mktime
        def get_time(mtime):
            from pytz import utc
            if mtime == '':
                mtime = datetime.now()
            ts = mktime(utc.localize(mtime).utctimetuple())
            return int(ts)

        try:
            if global_para is not None:
                result = eval(p, global_para, locals())
            else:
                result = eval(p)
        except Exception as e:
            p_expt(e)
        return result

    def __str__(self):
        return get_type_name(self) + '\t' + to_str(self.p)


class Transformer(ETLTool):
    '''
      base class for all transformer
    '''

    def __init__(self):
        super(Transformer, self).__init__()
        self.one_input = False
        self._m_process = False

    def transform(self, data):
        pass

    def m_process(self, data, column):
        for r in data:
            yield r

    def _process(self, data, column, transform_func):
        def edit_data(col, n_col=None):
            n_col = n_col if n_col != '' and n_col is not None  else col
            if col != '' and not has(data, col) and (not isinstance(self, (SetTF, MapTF))):
                return data
            if self.one_input:
                try:
                    res = transform_func(get_value(data, col))
                    return set_value(data, n_col, res)
                except Exception as e:
                    p_expt(e)
            else:
                n_col = n_col if n_col != '' and n_col is not None else col
                if col != '' and col not in data and isinstance(col, (SetTF,)):
                    return
                try:
                    transform_func(data, col, n_col)
                    return data
                except Exception as e:
                    p_expt(e)

        if is_str(column):
            if column.find(u':') >= 0:
                column = para_to_dict(column, ' ', ':')
            elif column.find(' ') > 0:
                column = [r.strip() for r in column.split(' ')]
        if isinstance(column, dict):
            for k, v in column.items():
                edit_data(k, v)
        elif isinstance(column, (list, set)):
            for k in column:
                edit_data(k)
        else:
            return edit_data(column, None)
        return data

    def process(self, generator, env):
        column = env['column']
        if self._m_process == True:
            for r in self.m_process(generator, column):
                yield r
            return
        if generator is None:
            return
        for d in generator:
            if d is None:
                continue
            if debug_level > 3:
                logging.debug(d)
                logging.debug('transformer:' + get_type_name(self))
            res = self._process(d, column, self.transform)
            yield res


class Executor(ETLTool):
    '''
        base class for all executor
      '''

    def __init__(self):
        super(Executor, self).__init__()
        self.debug = False

    def execute(self, data, column):
        pass

    def process(self, data, env):
        column = env['column']
        for r in data:
            yield self.execute(r, column)

    def init(self):
        pass


class Filter(ETLTool):
    '''
        base class for all filter
    '''

    def __init__(self):
        super(Filter, self).__init__()
        self.reverse = False
        self.stop_while = False
        self.one_input = True

    def filter(self, data):
        return True

    def process(self, data, env):
        column = env['column']
        error = 0
        for r in data:
            item = None
            result = False
            if self.one_input:
                if column in r:
                    item = r[column]
                    result = self.filter(item)
                if item is None and self.__class__ != NotNull:
                    continue
            else:
                item = r
                result = self.filter(item, column)

            if result == True and self.reverse == False:
                yield r
            elif result == False and self.reverse == True:
                yield r
            else:
                error += 1
                if self.stop_while == False:
                    continue
                elif self.stop_while == True:
                    logging.info('stop iter \n')
                    break
                elif isinstance(self.stop_while, int) and error >= self.stop_while:
                    logging.info('stop iter \n')
                    break


class Ascend(ETLTool):
    '''
    ascend sorter
    :param p: sort lambda function or str
    '''

    def __init__(self):
        super(Ascend, self).__init__()
        self._reverse = False
        self.p = '_'
        self.one_input = True

    def process(self, data, env):
        column = env['column']
        all_data = [r for r in data]
        p = 'value' if self.p == '' else  self.p

        def sort_map(data):
            import inspect
            if column == '':
                c = data
            else:
                c = data.get(column, None)
            if is_str(p):
                dic = merge({'_': c}, data)
                result = self._eval_script(p, dic)
            elif inspect.isfunction(p):
                result = p(c)
            return result

        all_data.sort(key=lambda x: sort_map(x), reverse=self._reverse)
        for r in all_data:
            yield r


class Descend(Ascend):
    '''
      descend sorter
      :param p: sort lambda function or str
    '''

    def __init__(self):
        super(Descend, self).__init__()
        self._reverse = True


class Generator(ETLTool):
    def __init__(self):
        super(Generator, self).__init__()
        self.mode = MERGE_APPEND
        self.pos = 0

    def generate(self, data, env):
        pass

    def process(self, generator, env):
        column = env['column']
        if generator is None:

            return self.generate(None, env)
        else:
            p = self.mode
            if p == MERGE_APPEND:
                return append(generator, self.process(None, env))
            elif p == MERGE_MERGE:
                return merge(generator, self.process(None, env))
            elif p == MERGE_CROSS:
                return cross(generator, self.generate, env)
            else:
                return mix(generator, self.process(None, env))


EXECUTE_INSERT = 'insert'
EXECUTE_SAVE = 'save'
EXECUTE_UPDATE = 'update'


class MongoDBConnector(EObject):
    def __init__(self):
        super(MongoDBConnector, self).__init__()
        self.connect_str = ''
        self.db = ''

    def init(self):
        import pymongo
        client = pymongo.MongoClient(self.connect_str)
        self._db = client[self.db]


class DBBase(ETLTool):
    def __init__(self):
        super(DBBase, self).__init__()
        self.table = ''
        self.mode = EXECUTE_INSERT

    def get_table(self, data):
        c = query(data, self.p)
        t = query(data, self.table)
        connector = self._proj.env[c]
        connector.init()
        table = connector._db[t]
        return table


class DbEX(Executor, DBBase):
    '''
    db writer and updater
    :param p:  env connector name
    '''

    def __init__(self):
        super(DbEX, self).__init__()

    def init(self):
        DBBase.init(self)

    def process(self, datas, env):
        column = env['column']
        for data in datas:
            table = self.get_table(data)
            work = {EXECUTE_INSERT: lambda d: table.save(d), EXECUTE_UPDATE: lambda d: table.save(d)}
            new_data = copy(data)
            etype = query(data, self.mode)
            work[etype](new_data)
            yield data


class DbGE(Generator, DBBase):
    '''
    db reader
    :param p:  env connector name
    '''

    def generate(self, data, env):
        column = env['column']
        table = self.get_table(self.table)
        for data in table.find():
            yield data


class JoinDBTF(Transformer, DBBase):
    '''
    db join
    :param p:  env connector name
    :param mapper: column value mapper
    :param index:  join index key
    '''

    def __init__(self):
        super(JoinDBTF, self).__init__()
        self.index = ''
        self.mapper = ''

    def transform(self, data, col, ncol):
        table = self.get_table(data)
        if table is None:
            buf = []
        else:
            value = data[self.index]
            mapper = query(data, self.mapper)
            mapper = para_to_dict(mapper, ' ', ':')

            def db_filter(d):
                if '_id' in d:
                    del d['_id']

            keys = {r: 1 for r in mapper.keys()}
            result = table.find({self.index: value}, keys)
            buf = []
            for r in result:
                db_filter(r)
                r = conv_dict(r, mapper)
                buf.append(r)
        data[ncol] = buf


class MatchFT(Filter):
    '''
      filter that match keyword or regex
      :param p:  keyword or regex
      :param count: min match count
      '''

    def __init__(self):
        super(MatchFT, self).__init__()
        self.regex = ''

    def init(self):
        if self.regex != '':
            self.regex = re.compile(self.p)
        self.count = 1

    def filter(self, data):
        p = self.get_p(data)
        if self.regex == '':
            return data.find(p) >= 0
        else:
            v = self.regex.findall(data)
            if v is None:
                return False
        return self.count <= len(v)


class NotIn(Filter):
    def __init__(self):
        super(NotIn, self).__init__()
        self.one_input = False

    def filter(self, data, column):
        item = data[column]
        p = self.get_p(data)
        if isinstance(p, (list, tuple)):
            return item not in p
        elif is_str(item):
            sp = to_str(p).split(':')
            if len(sp) == 1:
                p = query(data, p)
                return p != item
            else:
                min = float(query(data, sp[0]))
                max = float(query(data, sp[1]))
                return min <= item <= max
        else:
            return False


class RepeatFT(Filter):
    '''
        filter that key column repeated
    '''

    def __init__(self):
        super(RepeatFT, self).__init__()

    def init(self):
        self._set = set()

    def filter(self, data):
        if data in self._set:
            return False
        else:
            self._set.add(data)
            return True


class NotNull(Filter):
    '''
    filter that key column is empty or None
    '''

    def filter(self, data):
        if data is None:
            return False
        if is_str(data):
            return data.strip() != ''
        return True


# class DebugTF(Transformer):
#     def __init__(self):
#         super(DebugTF, self).__init__()
#     def process(self, generator, env):
#         env['execute']=not self.p
#         return super(DebugTF,self).process(generator,env)


class SetTF(Transformer):
    '''
    set value
    :param p: target value
    '''

    def __init__(self):
        super(SetTF, self).__init__()

    def transform(self, data, col, ncol):
        p = self.get_p(data)
        data[col] = p


class LetTF(Transformer):
    '''
    make stream target on certain column.
    :param p: column name
    '''

    def __init__(self):
        super(LetTF, self).__init__()
        self.regex = ''

    def m_process(self, generator, env):
        return generator


class CountTF(Transformer):
    def __init__(self):
        super(CountTF, self).__init__()
        self._m_process = True

    def m_process(self, generator, column):
        count = 0
        for data in generator:
            yield data
            count += 1
            if count % self.p == 0:
                print(count)


class IncrTF(Transformer):
    '''
    add a auto increase value
    :param p: useless
    '''

    def init(self):
        super(IncrTF, self).__init__()
        self._index = 0

    def transform(self, data):
        self._index += 1
        return self._index


class TagTF(Transformer):
    '''
      add a auto increase value
      :param p: useless
    '''
    pass


class CopyTF(Transformer):
    '''
    copy one or more columns to other columns, then target column will move, diff from cp2.  (cp short for copy)
    :param p: like 'a:b c:d' means copy a to b, and copy c to d
              if have target column a, can short as ':b', equal as 'a:b'
    '''

    def __init__(self):
        super(CopyTF, self).__init__()

    def transform(self, data, col, ncol):
        v = get_value(data, col)
        set_value(data, ncol, v)


class Copy2TF(CopyTF):
    '''
       copy one or more columns to other columns, then target column will not move, diff from cp
       :param p: like 'a:b c:d' means copy a to b, and copy c to d
                 if have target column a, can short as ':b', equal as 'a:b'
       '''
    pass


class Copy3TF(CopyTF):
    '''
       copy one or more columns to other columns, then target column will not move, diff from cp
       :param p: like 'a:b c:d' means copy a to b, and copy c to d
                 if have target column a, can short as ':b', equal as 'a:b'
       '''
    pass


class MoveTF(Transformer):
    '''
    move one or more columns to other columns, mv short for move
    :param p: like 'a:b c:d' means move a to b, and copy c to d
    '''

    def transform(self, data, col, ncol):
        if col != ncol:
            set_value(data, ncol, get_value(data, col))
            del_value(data, col)


class RemoveTF(Transformer):
    '''
    delete certain columns, rm short for move
    :param p: like 'a b c', will delete column a,b,c
    '''

    def __init__(self):
        super(RemoveTF, self).__init__()
        self.regex = ''

    def transform(self, data, col, ncol):
        del_value(data, col)


class KeepTF(Transformer):
    '''
    keep certain columns, delete all other columns
    :param p: like 'a b c', will keep column a,b,c
    '''

    def __init__(self):
        super(KeepTF, self).__init__()
        self._m_process = True
        self.regex = ''

    def m_process(self, datas, col):
        if isinstance(col, dict):
            for data in datas:
                doc = {}
                for k, v in data.items():
                    if k in col:
                        doc[col[k]] = v
                yield doc
        else:
            for data in datas:
                doc = {}
                for k, v in data.items():
                    if k in col:
                        doc[k] = v
                yield doc


class EscapeTF(Transformer):
    '''
    escape string
    :param p: , mode can be 'html','url' or 'text', default is 'url'
    '''

    def __init__(self):
        super(EscapeTF, self).__init__()
        self.one_input = True
        self.p = 'html'

    def transform(self, data):
        p = self.get_p(data)
        if p == 'html':
            if PY2:
                import cgi
                return cgi.escape(data)
            else:
                return html.escape(data)
        elif p == 'url':
            url = data.encode('utf-8')
            return urllib.parse.quote(url)
        return data


class CleanTF(Transformer):
    '''
    un_escape or clean string
    :param p:  mode can be 'html','url' or 'text', default is 'html'
    '''

    def __init__(self):
        super(CleanTF, self).__init__()
        self.one_input = True
        self.p = 'text'

    def transform(self, data):
        p = self.get_p(data)
        if p == 'text':
            if PY2:
                return data.encode('utf-8').decode('string_escape').decode('utf-8').replace('\'', '\"')
            else:
                return data.decode('unicode_escape')
        elif p == 'html':
            if PY2:
                import HTMLParser
                html_parser = HTMLParser.HTMLParser()
                return html_parser.unescape(data)
            else:
                return html.unescape(data)
        elif p == 'url':
            return urllib.parse.unquote(data)
        return data


class FormatTF(Transformer):
    '''
    format string,like python format
    :param p: like 'ab{0}c{column_name}def' ,'{0}' represent current column
    '''

    def __init__(self):
        super(FormatTF, self).__init__()
        self.p = '{0}'
        self._re = re.compile('\{([\w_]+)\}')

    def transform(self, data, col, ncol=None):
        input = self.p
        if col not in [None, '']:
            dic = merge({'_': data[col]}, data)
        else:
            dic = data
        output = input.format(**dic)
        data[ncol] = output


class HtmlTF(Transformer):
    '''
    get html text from a node, if not node, will not change it
    '''

    def __init__(self):
        super(HtmlTF, self).__init__()
        self.one_input = True

    def transform(self, data):
        from pyquery import PyQuery
        if isinstance(data, PyQuery):
            return data.html()
        res = get_node_html(data)
        return res


class TextTF(Transformer):
    '''
    get html text from a node, if not node, will not change it
    '''

    def __init__(self):
        super(TextTF, self).__init__()
        self.one_input = True

    def transform(self, data):
        from pyquery import PyQuery
        if isinstance(data, PyQuery):
            return '\n'.join(get_node_text(r) for r in data)
        res = get_node_text(data)
        if res == '':
            return to_str(data)
        return res


class RegexTF(Transformer):
    '''
    get regex match results from string into target array
    '''

    def __init__(self):
        super(RegexTF, self).__init__()
        self.one_input = True

    def m_str(self, data):
        if is_str(data):
            return data
        elif isinstance(data, (list, tuple)):
            return ''.join(data)
        return data

    def _conv(self, x):
        return x

    def transform(self, data):
        regex = re.compile(self.p)
        items = re.findall(regex, to_str(data))
        return [self._conv(self.m_str(r)) for r in items]


class LastTF(Transformer):
    '''
     get last row from  stream
    '''

    def __init__(self):
        super(LastTF, self).__init__()
        self.count = 1
        self._m_process = True

    def m_process(self, data, column):
        r0 = None
        while True:
            try:
                # 获得下一个值:
                x = next(data)
                r0 = x
            except StopIteration:
                # 遇到StopIteration就退出循环
                break
        yield r0


class AggTF(Transformer):
    '''
    aggregate current row with next row
    :param p: aggregate function, can be lambda, function or string, parameter is a,b
    '''

    def __init__(self):
        super(AggTF, self).__init__()
        self._m_process = True

    def m_process(self, data, column):
        p = self.get_p(data)
        r0 = None
        import inspect
        for m in data:
            if column != '':
                r = m[column]
            if r0 == None:
                r0 = r
                yield m
                continue
            if is_str(p):
                r2 = self._eval_script(p, global_para={'a': r0, 'b': r})
            elif inspect.isfunction(p):
                r2 = p(r0, r)
            if r2 != None:
                r = r2
            if column != '':
                m[column] = r
            else:
                m = r
            yield m
            r0 = r


class ReplaceTF(RegexTF):
    '''
    replace string
    :param p: match str or regex
    :param mode: 'str' or 're'
    :param value: new value
    '''

    def __init__(self):
        super(ReplaceTF, self).__init__()
        self.value = ''
        self.mode = 'str'
        self.one_input = False

    def transform(self, data, col, ncol):
        ndata = data[col]
        if ndata is None:
            return
        new_value = query(data, self.value)
        p = self.get_p(data)
        if self.mode == 're':
            result = re.sub(p, new_value, ndata)
        else:
            if ndata is None:
                result = None
            else:
                result = ndata.replace(p, new_value)
        data[ncol] = result


class NumTF(RegexTF):
    '''
    get number from str
    '''

    def __init__(self):
        super(NumTF, self).__init__()
        # self.p='(-?\d+)(\.\dt+)?'
        self.p = '(-?\\d+)(\\.\\d+)?'

    def _conv(self, x):
        try:
            return int(x)
        except Exception as e:
            return float(x)


class SplitTF(Transformer):
    '''
    split value with certain chars
    :param p: 'a b': split string with a,b, return str array
    '''

    def __init__(self):
        super(SplitTF, self).__init__()
        self.one_input = True

    def transform(self, data):
        return re.split(self.p, data)


class IntoTF(Transformer):
    def transform(self, data, col, ncol):
        v = data[col]
        p = self.get_p(data)
        items = replace_paras(split(p, ' '), col)
        if isinstance(v, list):
            for i in range(min(len(v), len(items))):
                data[items[i]] = v[i]


class StripTF(Transformer):
    '''
    strip string with certain char
    :param p: char
    '''

    def __init__(self):
        super(StripTF, self).__init__()
        self.one_input = True

    def transform(self, data):
        if data is None:
            return None
        p = self.get_p(data)
        if p == '':
            return data.strip()
        else:
            return data.strip(p)


class ExtractTF(Transformer):
    '''
    get substring starts with 'start' and ends with 'end' from string
     :param p: start string
     :param end: end string
     :param has_margin: bool, if contain start and end
     '''

    def __init__(self):
        super(ExtractTF, self).__init__()
        self.has_margin = False
        self.one_input = True
        self.end = ''

    def transform(self, data):
        start = data.find(self.p)
        if start == -1:
            return
        end = data.find(self.end, start)
        if end == -1:
            return
        if self.has_margin:
            end += len(self.end)
        if not self.has_margin:
            start += len(self.p)
        return data[start:end]


class MapTF(Transformer):
    def __init__(self):
        super(MapTF, self).__init__()
        self.p = 'script'

    def _get_data(self, data, col):
        p = self.get_p(data)
        if col == '':
            if is_str(p):
                dic = merge({'_': data}, data)
                self._eval_script(p, dic)
            else:
                p(data)
            return None
        else:

            value = data[col]
            if is_str(p):
                dic = merge({'_': value}, data)
                result = self._eval_script(p, dic)
            else:
                result = p(value)
        return result

    def transform(self, data, col, ncol):
        js = self._get_data(data, col)
        if ncol != '':
            data[ncol] = js


class Create(Generator):
    def __init__(self):
        super(Create, self).__init__()
        self.p = 1

    def can_dump(self):
        return is_str(self.p)

    def generate(self, data, env):
        column = env['column']
        from pandas import DataFrame
        p = self.get_p(data)
        import inspect
        import copy
        if is_str(p):
            if p == '':
                result = [{}]
            else:
                result = self._eval_script(p)
        elif isinstance(p, int):
            result = ({} for i in range(p))
        elif inspect.isfunction(p):
            result = p()

        elif isinstance(p, DataFrame):
            result = (row.to_dict() for l, row in p.iterrows())
        else:
            result = p
        for r in result:
            if column != '':
                yield {column: r}
            else:
                yield copy.copy(r)


class Where(Filter):
    def __init__(self):
        super(Where, self).__init__()
        self.p = 'True'
        self.one_input = False

    def can_dump(self):
        return is_str(self.p)

    def filter(self, data, column):
        p = self.get_p(data)
        import inspect
        data = copy(data)
        if column == '':
            value = data
        else:
            value = data[column] if column in data else ''
        if is_str(p):
            dic = merge({'_': data}, data)
            result = self._eval_script(p, dic)
        elif inspect.isfunction(p):
            result = p(value)
        if result == None:
            return False
        return result


class DetectTF(Transformer):
    def __init__(self):
        super(DetectTF, self).__init__()
        self.attr = False
        self.index = 0
        self._m_process = True

    def m_process(self, datas, column):
        from lxml import etree
        is_first = True
        for data in datas:
            p = self.get_p(data)
            root = data[column]
            if root is None:
                return
            if is_str(root):
                root = _get_etree(root)
            tree = etree.ElementTree(root)
            if p != '':
                xpaths = get_diff_nodes(tree, root, self.p, self.attr)
                root_path = p
            else:
                result = first_or_default(get_mount(search_properties(root, None, self.attr), take=1, skip=self.index))

                if result is None:
                    print('great hand failed')
                    return
                root_path, xpaths = result
            for r in xpaths:
                r.path = get_sub_xpath(root_path, r.path)

            code0 = '\n.'.join((u"cp('{ncol}:{col}').xpath('/{path}')[0].text()\\".format(col=r.name, ncol=column,
                                                                                          path=r.path, sample=r.sample)
                                for r in xpaths))
            if is_first:
                code = u".xpath('%s').list().html().tree()\\\n.%s" % (root_path, code0)
                print(code)
                code2 = '\n'.join(
                    u"#{key} : #{value}".format(key=r.name, value=r.sample.strip() if r.sample is not None else '') for
                    r in xpaths)
                print(code2)
                is_first = False
            result = get_datas(root, xpaths, True, root_path=root_path)
            for r in result:
                yield r


class CacheTF(Transformer):
    def __init__(self):
        super(CacheTF, self).__init__()
        self._m_process = True

    def m_process(self, datas, column):
        i = 0
        cache = self.p
        if cache is None:
            for data in datas:
                yield data
            return
        while i < len(cache):
            yield cache[i]
            i += 1
        del cache[:]
        for r in datas:
            cache.append(r)
            yield r


class WebTF(Transformer):
    def __init__(self):
        super(WebTF, self).__init__()
        self.encoding = 'utf-8'
        self._mode = 'get'
        self.header = ''

    def transform(self, data, col, ncol):
        import requests
        from etlpy.spider import get_encoding
        if self.p in [None, '']:
            req = request_param.copy()
            key = 'data' if self._mode == 'post' else 'url'
            req[key] = ExpParam('[_]')
        else:
            req = self.p

        result = None
        real_req = req.get(data, col)
        if 'url' in real_req:
            url =str(real_req['url'])
            if not url.startswith('http'):
                url = 'http://'+url
            real_req['url'] = url
        cache = self._proj.cache
        if cache is not None:
            key = real_req.get('url', '') + real_req.get('data', '')
            result = cache.get(key, None)
        if result is None:
            try:

                if self._mode == 'get':
                    r = requests.get(**real_req)
                else:
                    r = requests.post(**real_req)
                r.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
                if r is not None and r.status_code == 200:
                    result = get_encoding(r.content)
                    if cache is not None and cache.size() < 1000:
                        cache.set(key, result)
            except requests.RequestException as e:
                p_expt(e)

        data[ncol] = result


class GetTF(WebTF):
    def __init__(self):
        super(GetTF, self).__init__()
        self._mode = 'get'


class PostTF(WebTF):
    def __init__(self):
        super(PostTF, self).__init__()
        self._mode = 'post'


class TreeTF(Transformer):
    def __init__(self):
        super(TreeTF, self).__init__()
        self.one_input = True
        self.smart = False

    def transform(self, data):
        from lxml.etree import iselement
        if not iselement(data):
            root = _get_etree(data)
        else:
            root = data
        if self.smart:
            return get_main(root)
        return root


class SearchTF(Transformer):
    def __init__(self):
        super(SearchTF, self).__init__()
        self.one_input = True
        self.mode = 'str'

    def transform(self, data):
        if data is None:
            return None
        if is_str(data):
            from lxml import etree
            tree = _get_etree(data)
        result = search_xpath(tree, self.p, self.mode, True)
        print(result)
        return result


class ListTF(Transformer):
    def __init__(self):
        super(ListTF, self).__init__()
        self._m_process = True

    def m_process(self, generator, col):
        for data in generator:
            if data is None or col not in data:
                if debug_level > 0:
                    logging.warn('data empty')
                    continue
            root = data[col]
            if self.p == '*':
                p = list(data.keys())
                p.remove(col)
            else:
                p = self.get_p(data)
            for r in root:
                r2 = {col: r}
                my = merge_query(r2, data, p)
                yield my


class XPathTF(Transformer):
    def _trans(self, data):
        from lxml import etree
        root = None
        if is_str(data):
            root = _get_etree(data)
            tree = etree.ElementTree(root)
        else:
            tree = data
        return tree, root

    def transform(self, data, col, ncol):
        target = data[col]
        tree, root = self._trans(target)
        if tree is None:
            return
        node_path = query(data, self.p)
        if node_path is None or node_path == '':
            nodes = [target]
        else:
            nodes = tree.xpath(self.p)
            if nodes is None:
                nodes = [target]
        data[ncol] = nodes


class StopTF(Transformer):
    def __init__(self):
        super(Transformer, self).__init__()


class PyQTF(XPathTF):
    def __init__(self):
        super(PyQTF, self).__init__()

    def transform(self, data, col, ncol):
        from pyquery import PyQuery as pyq
        root = pyq(data[col])
        if root is None:
            return
        node_path = self.get_p(data)
        if node_path == '' or node_path is None:
            return
        nodes = root(node_path)
        data[ncol] = nodes if nodes is not None else []


class AtTF(Transformer):
    def __init__(self):
        super(AtTF, self).__init__()
        self.one_input = True

    def transform(self, data):
        p = self.get_p(data)
        if isinstance(self.p, slice):
            return data[p]
        elif isinstance(data, (list, tuple)):
            p = get_num(p, 0)
            if len(data) <= p:
                return None
            return data[p]
        elif data is None:
            return None
        else:
            return data.get(p, None)


class ParallelTF(Transformer):
    def __init__(self):
        super(ParallelTF, self).__init__()
        self.p = NORMAL_MODE
        self.worker_num = 1



class DumpTF(Transformer):
    def __init__(self):
        super(DumpTF, self).__init__()
        self.one_input = True
        self.p = 'json'

    def transform(self, data):
        p = self.get_p(data)
        if p == 'json':
            return json.dumps(data)

        elif p == 'yaml':
            import yaml
            return yaml.dumps(data)
        elif p == 'html':
            return get_node_html(data)
        else:
            return to_str(data)


class LoadTF(Transformer):
    def __init__(self):
        super(LoadTF, self).__init__()
        self.one_input = True
        self.p = 'json'

    def transform(self, data):
        p = self.p
        if p == 'json':
            return json.loads(data)
        elif p == 'yaml':
            import yaml
            return yaml.loads(data)
        elif p == 'html':
            return _get_etree(data)


class Range(Generator):
    def __init__(self):
        super(Range, self).__init__()
        self.p = ':'

    def generate(self, data, env):
        column = env['column']
        p = self.get_p(data)
        start = 0
        interval = 1
        if is_str(p):
            start, end, interval = get_range(p, data)
        elif isinstance(p, int):
            end = p
        elif isinstance(p, (tuple, list)):
            l = len(p)
            if l > 0:
                start = p[0]
            if l > 1:
                end = p[1]
            if l > 2:
                interval = p[2]
        if start == end:
            yield {column: start}

            return
        values=[]
        try:
            values = range(start, end, interval)
        except Exception as e:
            pass
        for i in values:
            item = {column: i}
            yield item


class SubBase(ETLTool):
    def __init__(self):
        super(SubBase, self).__init__()
        self.range = '0:100'

    def _get_task(self, data):
        p = self.get_p(data)
        if isinstance(p, ETLTask):
            return p
        if p not in self._proj.env:
            sys.stderr.write('sub task %s  not in current project' % p)
        sub_etl = self._proj.env[p]
        return sub_etl

    def _get_tools(self, data):
        sub_etl = self._get_task(data)
        start, end, interval = get_range(self.range)
        tools = tools_filter(sub_etl.tools[start:end:interval], excluded=self, mode=NORMAL_MODE)
        return tools

    def _generate(self, data, env):
        doc = copy(data)
        generator = [doc] if doc is not None else None
        for r in ex_generate(self._get_tools(doc), generator, env):
            yield r


class SubGE(Generator, SubBase):
    def __init__(self):
        super(SubGE, self).__init__()

    def generate(self, data, env):
        return self._generate(data, env)


class SubEx(Executor, SubBase):
    def __init__(self):
        super(SubEx, self).__init__()

    def process(self, data, env):
        for d in data:
            if self.debug == True:
                for r in self._generate(d, env):
                    yield r
            else:
                yield self.execute(d, env)

    def execute(self, data, env):
        count = 0
        try:
            for r in self._generate(data, env):
                count += 1
        except Exception as e:
            p_expt(e)
        print('subtask:' + to_str(count))
        return data


class SubTF(Transformer, SubBase):
    def __init__(self):
        super(SubTF, self).__init__()

    def transform(self, data, col, ncol):
        env = {'column': col}
        data[ncol] = (r for r in self._generate(data, env))


class RotateTF(Transformer):
    '''
    rotate matrix, not lazy
    '''

    def __init__(self):
        super(RotateTF, self).__init__()
        self._m_process = True

    def m_process(self, datas, column):
        result = {}
        for data in datas:
            p = self.get_p(data)
            key = data.get(column, None)
            if key is None:
                continue
            value = query(data, p)
            result[key] = value
        yield result


class ToDictTF(Transformer):
    '''
    take certain columns merge into dict
    :param p: columns, split by blanket
    '''

    def __init__(self):
        super(ToDictTF, self).__init__()
        self._m_process = True

    def m_process(self, datas, col):
        for data in datas:
            doc = {}
            merge_query(doc, data, self.p)
            data[col] = doc
            yield data


class DrillTF(Transformer):
    '''
    take dict value out
    :param p: dict columns, split by blanket.  all column will be added if value is ''
    '''

    def __init__(self):
        super(DrillTF, self).__init__()
        self._m_process = True

    def m_process(self, datas, col):
        for data in datas:
            doc = copy(data)
            col_data = data[col]
            if self.p != '':
                merge_query(doc, col_data, self.p)
            else:
                merge(doc, col_data)
            yield doc


class TakeTF(Transformer):
    '''
    take top n rows
    :param p: n
    '''

    def __init__(self):
        super(TakeTF, self).__init__()

    def process(self, data, env):
        column = env['column']
        p = get_num(self.get_p(data))
        for r in get_mount(data, p):
            yield r


class SkipTF(Transformer):
    '''
    skip n rows
    :param p: n
    '''

    def __init__(self):
        super(SkipTF, self).__init__()

    def process(self, data, column):
        p = get_num(self.get_p(data))
        for r in get_mount(data, None, p):
            yield r


class DelayTF(Transformer):
    '''
     delay some time
     :param p: delay n millisecond
     '''

    def __init__(self):
        super(DelayTF, self).__init__()
        self.p = 100
        self._m_process = True

    def m_process(self, datas, col):
        import time
        for data in datas:
            p = get_num(self.get_p(data), default=0)
            time.sleep(float(p) / 1000.0)
            yield data


class Read(Generator):
    pass


class Download(Executor):
    '''
    download a file from web
    :param p: target url
    :param path: save path on disk
    '''

    def __init__(self):
        super(Download, self).__init__()
        self.path = ''

    def execute(self, data, column):
        import requests
        p = self.get_p(data)
        (folder, file) = os.path.split(p)
        if not os.path.exists(folder):
            os.makedirs(folder)
        url = data[column]
        target = open(p, 'wb')
        req = requests.get(url)
        if req is None:
            return
        target.write(req.content)
        target.close()
        return data


class Project(EObject):
    '''
    project that contains all tasks
    '''

    def __init__(self):
        self.env = {}
        self.desc = "edit project description here"
        if ALLOW_CACHE:
            self.cache = pickledb(CACHE_DB_NAME, False)
        else:
            self.cache = None

    def clear(self):
        '''
        clear all tasks in project
        :return: self
        '''
        if self.cache is not None:
            self.cache.deldb()
        self.env.clear()
        return self

    def dumps_json(self):
        dic = convert_dict(self)
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def dumps_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def dump_yaml(self, path):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.dumps_yaml())

    def load_yaml(self, path):
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            d = yaml.load(f)
            return self.load_dict(d)

    def loads_json(self, js):
        d = json.loads(js)
        return self.load_dict(d)

    def dump_json(self, path):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.dumps_json())

    def load_json(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            js = f.read()
            return self.loads_json(js)

    def load_dict(self, dic):
        items = dic.get('env', {})
        for key, item in items.items():
            if 'Type' in item:
                item['Type'] = item['Type'].replace('tools.', '')
                obj_type = item['Type'].replace('tools.', '')
                task = eval('%s()' % obj_type)
                if obj_type == 'ETLTask':
                    for r in item['tools']:
                        r['Type'] = r['Type'].replace('tools.', '')
                        etl = eval('%s()' % r['Type'])
                        for attr, value in r.items():
                            if attr in ['Type']:
                                continue
                            setattr(etl, attr, value)
                        etl._proj = self
                        task.tools.append(etl)
                else:
                    dict_copy_poco(task, item)
            else:
                task = item
            self.env[key] = task
        return self


def ex_generate(tools, generator=None, env=None):
    if env is None:
        env = {'column': '', 'execute': False}
    start = 0
    while True:
        take_index = get_index(tools[start:], lambda x: isinstance(x, TakeTF))
        if take_index == -1:
            break
        else:
            generator = _ex_generate(tools[start:start + take_index], generator, env)
            start += take_index + 1
    pos = 0 if start == 0 else start - 1
    generator = _ex_generate(tools[pos:], generator, env)
    for item in generator:
        yield item


def _ex_generate(tools, generator=None, env=None):
    mapper, reducer, pl = parallel_map(tools, env)
    if pl is None:
        group, mode, worker_num = 1, None, 1
    else:
        group, mode, worker_num = 1, pl.p, pl.worker_num
    #TODO: 对group的考虑
    generator = generate(mapper, generator, env)
    if reducer is None:
        return generator
    # group_generator = group_by_mount(generator, group)
    generator = multi_yield(lambda task: _ex_generate(reducer, task, env), mode, worker_num, generator)
    return generator


def tools_filter(tools, init=True, excluded=None, mode=NORMAL_MODE):
    stop_index = get_index(tools, lambda x: isinstance(x, StopTF))
    if stop_index != -1:
        tools = tools[:stop_index]

    buf = []
    if not isinstance(mode, (list, tuple)):
        mode = [i for i in range(mode, -1, -1)]
    else:
        mode.sort(reverse=False)
    if mode[-1] != NORMAL_MODE:
        mode.append(NORMAL_MODE)
    index = 0
    for tool in tools:
        if excluded == tool:
            continue
        if init:
            tool.init()
        buf.append(tool)

    buf2 = []
    total = len(buf)
    # 若pl设置的级别比全局级别高，要降级
    # 若pl比全局级别低，则以该级别为准
    # 级别越往后走越低，没有同级的，最低到0

    for i in range(total):
        tool = buf[i]
        allow_insert = False
        buf2.append(tool)
        if isinstance(tool, ListTF):
            j = i + 1
            while j < total and not isinstance(buf[j], ParallelTF):
                if isinstance(buf[j], ListTF):
                    allow_insert = True
                    break
                j += 1
            pl = None
            if j == total:
                pass
            elif allow_insert:
                pl = ParallelTF()
                buf2.append(pl)
            elif isinstance(buf[j], ParallelTF):
                pl = buf[j]
            if pl is not None:
                pl.mode = mode[index]
                pl.worker_num = DEFAULT_WORKER_NUM if pl.p < NETWORK_MODE else 1
                if index < len(mode) - 1:
                    index += 1
    return buf2


def get_column(tool, env):
    next_column = old_column = column = env['column']
    p = tool.p
    if is_str(p):
        if p.find(u':') >= 0:
            p = para_to_dict(p, ' ', ':')

        elif p.find(' ') > 0:
            p = [r.strip() for r in p.split(' ')]
        p = replace_paras(p, old_column)
    if isinstance(tool, Copy3TF):
        if env.get('keep', None) is None:
            env['keep'] = old_column
            column = {column: p}
        else:
            column = {env['keep']: p}
        next_column = p

    elif isinstance(tool, (LetTF, RemoveTF, KeepTF)):
        if tool.regex != '':
            pass
        column = p
        env['keep'] = None
    elif isinstance(tool, (CopyTF, MoveTF, Copy2TF)):
        column = p
        if isinstance(p, dict):
            if not isinstance(tool, Copy2TF):
                p2 = p.values()
            else:
                p2 = p.keys()

        next_column = ' '.join(p2)
        env['keep'] = None

    env['next'] = next_column
    env['column'] = column
    return env


def generate(tools, generator=None, env=None):
    '''
    evaluate a tool stream
    :param tools: [ETLTool]
    :param generator: seed generator for generator
    :param init: bool, if initiate every tool
    :param env:
    :return: a generator
    '''
    if tools is not None:
        for tool in tools:
            env = get_column(tool, env)
            if isinstance(tool, LetTF):
                continue
            execute = env.get('execute', False)
            if not execute and isinstance(tool, Executor):
                continue
            generator = tool.process(generator, env.copy())
            env['column'] = env['next']
    if generator is None:
        return []
    return generator


def parallel_map(tools, env, certain_mode=None):
    '''
    split tool into mapper and reducer
    :param tools:  a list for tool
    :return: mapper, reducer and parallel parameter
    '''

    index = get_index(tools, lambda x: isinstance(x, ParallelTF) and (certain_mode is None or x.p == certain_mode))
    if index == -1:
        return tools, None, None

    mapper = tools[:index]
    reducer = tools[index + 1:]
    parameter = tools[index]
    return mapper, reducer, parameter


class ETLTask(EObject):
    def __init__(self):
        self.tools = []
        self._master = None

    def __len__(self):
        return len(self.tools)

    def __tool_factory(self, tool_type, item):
        tool = tool_type()
        tool._proj = self._proj
        tool.p = item
        self.tools.append(tool)
        return tool

    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        if item == '_':
            item = ''
        tool = self.__tool_factory(LetTF, item)
        return self

    def __add__(self, task):
        if isinstance(task, ETLTask):
            for item in task.tools:
                item._proj = self._proj
                self.tools.append(item)
        # TODO: other kind of op?
        return self

    def __mul__(self, task):
        tool = self.__tool_factory(SubGE, task)
        tool.mode = MERGE_CROSS
        return self

    def __or__(self, task):
        tool = self.__tool_factory(SubGE, task)
        tool.mode = MERGE_MERGE
        return self

    def __getitem__(self, item):
        self.__tool_factory(AtTF, item)
        return self

    def to_json(self):
        dic = convert_dict(self)
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def to_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def eval(self, script=''):
        eval('self.' + script)
        return self

    def check(self):
        tools = to_list(tools_filter(self.tools))
        for i in range(1, len(tools)):
            attr = EObject()
            tool = tools[i]
            title = get_type_name(tool).replace('etl.', '') + ' ' + tool.column
            list_datas = to_list(progress_indicator(get_keys(ex_generate(tools[:i]), attr), title=title))
            keys = ','.join(attr.__dict__.keys())
            print('%s, %s, %s' % (str(i), title, keys))

    def distribute(self, port=None, monitor_connector_name=None, table_name=None):
        from etlpy.concurrence import Master
        self._master = Master(
            self._master.start_project(self._proj, self.name, port, monitor_connector_name, table_name))

    def _get_related_tasks(self, tasks):
        for r in self.tools:
            if isinstance(r, SubBase) and r not in tasks:
                tasks.add(r.name)
                r._get_related_tasks(tasks)

    def get_related_tasks(self):
        tasks = set()
        self._get_related_tasks(tasks)
        return tasks

    def pl_generator(self):
        new_proj = dill.dumps(self._proj)
        new_proj= codecs.encode(new_proj,'base64').decode()
        # for k, v in new_proj.items():
        #     if isinstance(v, dict) and v.get('Type', None) == 'ETLTask' and v['name'] not in related_tasks:
        #         del new_proj[k]

        def generator():
            env = {'column': '', 'execute': False}
            mapper, reducer, parallel = parallel_map(self.tools, NETWORK_MODE)
            if parallel is None:
                print('this script do not support pl...')
                return
            for jobs in group_by_mount(generate(mapper, None, env), parallel.worker_num):
                yield jobs, env

        id = 0
        for jobs, env in progress_indicator(generator()):
            job = {'proj': new_proj, 'name': self.name, 'tasks': jobs, 'id': id, 'env': env}
            yield job
            id += 1

    def rpc(self, method='finished', server='127.0.0.1', port=60007):
        import requests
        if method in ['finished', 'dispatched', 'clean']:
            url = "http://%s:%s/task/query/%s" % (server, port, method)
            data = requests.get(url).json()
            print('remain: %s' % (data['remain']))
            #return collect(data[method], count=1000000)
        elif method == 'insert':
            url = "http://%s:%s/task/%s" % (server, port, method)
            id = 0
            count=0
            for job in self.pl_generator():
                res = requests.post(url, json=job)
                js= res.json()
                print('task insert %s, %s' % (id,js))
                if js['status']=='success':
                    count+=1
                id += 1
            print('total push tasks: %s' % (count))

    def stop_server(self):
        if self._master is None:
            return 'server is not exist'
        self._master.manager.shutdown()

    def __str__(self):
        def conv_value(value):
            if is_str(value):
                value = value.replace('\n', ' ').replace('\r', '')
                sp = "'"
                if value.find("'") >= 0:
                    if value.find('"') >= 0:
                        sp = "'''"
                    else:
                        sp = '"'
                return "%s%s%s" % (sp, value, sp)
            return value

        array = []
        array.append('##task name:%s' % self.name)
        array.append('.clear()')
        for t in self.tools:
            typename = get_type_name(t)
            s = ".%s(%s" % (typename, conv_value(get_value(t, 'column')))
            attrs = []
            default_dict = type(t)().__dict__
            for att in t.__dict__:
                value = t.__dict__[att]
                if att in ['one_input']:
                    continue
                if not isinstance(value, (int, bool, float)) and not is_str(value):
                    continue
                if value is None or att not in default_dict or default_dict[att] == value:
                    continue
                attrs.append(',%s=%s' % (att.lower(), conv_value(value)))
            if any(attrs):
                s += ''.join(attrs)
            s += ')\\'
            array.append(s)
        return '\n'.join(array)

    def init(self):
        for tool in self.tools:
            tool.init()
        return self

    def todf(self):
        from pandas import DataFrame
        list_datas = to_list(self.query())
        return DataFrame(list_datas)

    def query(self, take=999, skip=0, mode=NORMAL_MODE):
        tools = tools_filter(self.init().tools[skip:take], init=False, mode=mode)
        for r in ex_generate(tools):
            yield r
        cache = self._proj.cache
        if cache is not None:
            cache.dump()

    def __iter__(self):
        for r in self.query():
            yield r

    def __call__(self, count=None, format=''):
        return self.collect(count=count, format=format)

    def debug(self, p):
        pass

    def collect(self, format='', count=None, paras=None):
        datas = get_mount(self.init().query(), take=count)
        return collect(datas, format, paras=paras)
