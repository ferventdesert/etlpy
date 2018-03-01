# encoding: UTF-8
import multiprocessing
import re
import sys
import logging
import cgitb

from ipy_progressbar import ProgressBar

PY2 = sys.version_info[0] == 2

enable_progress = True

if PY2:
    import codecs
    from Queue import Queue, Empty

    open = codecs.open
else:
    open = open
    from queue import Queue, Empty

debug_level = 4


def is_in_ipynb():
    try:
        from IPython import get_ipython
        cfg = get_ipython()
        return cfg is not None
    except NameError:
        return False


def set_level(level):
    debug_level = level
    if level > 0:
        cgitb.enable(format='text')


is_ipynb = is_in_ipynb()


def is_str(s):
    if PY2:
        if isinstance(s, (str, unicode)):
            return True
    else:
        if isinstance(s, (str)):
            return True
    return False


def to_str(s):
    if PY2 and isinstance(s, unicode):
        return s

    try:
        return str(s)
    except Exception as e:
        if PY2:
            return unicode(s)
        return 'to_str error:' + str(e)


def read_config(config):
    if isinstance(config, dict):
        new_config = Config()
        for k, v in config.items():
            new_config[k] = read_config(v)
        return new_config
    elif isinstance(config, list):
        for i in range(len(config)):
            config[i] = read_config(config[i])
    return config


class Config(dict):
    def __init__(self, dic=None):
        if dic is not None:
            self.read_config(dic)

    def read_config(self, config):
        dic2 = read_config(config)
        for k, v in dic2.items():
            self[k] = v

    def __getattr__(self, item):
        if item not in self:
            return None
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value


def get_range_mount(generator, start=None, end=None, interval=1):
    i = 0
    i2 = 0
    if interval == 0:
        interval = 1
    if isinstance(generator, list):
        generator = generator[start:end]
        for r in generator:
            yield r
    else:
        if start is None:
            start = -1
        if end is None:
            end = -1
        for r in generator:
            i += 1
            if i < start + 1:
                continue
            if end > 0 and i > end:
                break
            i2 += 1
            if i2 % interval == 0:
                yield r


def get_mount(generator, take=None, skip=0):
    i = 0
    for r in generator:
        i += 1
        if i < skip:
            continue
        if isinstance(take, int) and i > 0 and i > take + skip:
            break
        yield r


def foreach(generator, func):
    for r in generator:
        func(r)
        yield r


def concat(generators):
    for g in generators:
        for r in g:
            yield r


def to_list(generator, max_count=None):
    datas = []
    count = 0
    for r in generator:
        count += 1
        datas.append(r)
        if max_count is not None and count >= max_count:
            break
    return datas


def progress_indicator(generator, title='Position Indicator', count=2000):
    if not enable_progress:
        for r in generator:
            yield r
        return
    load = False
    try:
        # from ipy_progressbar import ProgressBar
        # load=True
        pass
    except Exception as e:
        p_expt(e)
    if is_ipynb and load:

        generator = ProgressBar(generator, title=title)
        generator.max = count
        generator.start()
        for data in generator:
            yield data
        generator.finish()
    else:
        id = 0
        for data in generator:
            id += 1
            yield data
        print('task finished')


def revert_invoke(item, funcs):
    for i in range(0, len(funcs), -1):
        item = funcs[i](item)
    return item


def s_invoke(func, **param):
    try:
        if debug_level > 2:
            logging.info('invoke' + str(func))
        return func(param)
    except Exception as e:
        p_expt(e)


def p_expt(e):
    if debug_level >= 3:
        logging.exception(e)
    elif debug_level < 2 and debug_level > 0:
        logging.error(e)
    else:
        pass


def collect(generator, format='print', paras=None):
    if format == 'print' and not is_ipynb:
        import pprint
        for d in generator:
            pprint.pprint(d)
        return
    elif format == 'keys':
        for d in generator:
            for k in paras:
                print("%s:  %s " % (k, d.get(k, 'None')))
    elif format == 'key':
        import pprint
        for d in generator:
            pprint.pprint(d.keys())
        return
    elif format == 'count':
        count = 0
        for d in generator:
            count += 1
        print('total count is ' + str(count))
    list_datas = to_list(progress_indicator(generator))
    if is_in_ipynb() or format == 'df':
        from  pandas import DataFrame
        return DataFrame(list_datas)
    else:
        return list_datas


def format(form, keys):
    res = form
    for i in range(len(keys)):
        res = res.replace('{' + to_str(i) + '}', to_str(keys[i]))
    return res


def get_keys(generator, s):
    count = 0
    for r in generator:
        count += 1
        if count < 5:
            for key in r.keys():
                if not key.startswith('_'):
                    try:
                        setattr(s, key, key)
                    except Exception as e:
                        pass
        yield r


def repl_long_space(txt):
    spacere = re.compile("[ ]{2,}")
    spacern = re.compile("(^\r\n?)|(\r\n?$)")
    r = spacere.subn(' ', txt)[0]
    r = spacern.subn('', r)[0]
    return r


def merge(d1, d2):
    for r in d2:
        d1[r] = d2[r]
    return d1


def conv_dict(dic, para_dic):
    import copy
    dic = copy.copy(dic)
    for k, v in para_dic.items():
        if k == v:
            continue
        if k in dic:
            dic[v] = dic[k]
            del dic[k]
    return dic


def replace_paras(item, old_value):
    def get_short(v):
        if v == '_':
            return old_value
        return v

    if isinstance(item, dict):
        p = {}
        for k, v in item.items():
            p[get_short(k)] = get_short(v)
        return p
    elif isinstance(item, list):
        for i in range(len(item)):
            item[i] = get_short(item[i])
        return item
    return item


def para_to_dict(para, split1, split2):
    r = {}
    for s in para.split(split1):
        s = s.strip()
        rs = s.split(split2)

        key = rs[0].strip()
        if len(rs) < 2:
            value = key
        else:
            value = s[len(key) + 1:].strip()
        if key == '':
            continue
        r[key] = value
    return r


def split(string, char):
    sp = string.split(char)
    result = []
    for r in sp:
        if r == '':
            continue
        result.append(r)
    return result


def get_num(x, method=int, default=None):
    if x in [None, '']:
        return None
    try:
        return method(x)
    except:
        if default is None:
            return x
        return default


def merge_query(d1, d2, columns):
    if is_str(columns) and columns.strip() != "":
        if columns.find(":") > 0:
            columns = para_to_dict(columns, ' ', ':')
        else:
            columns = columns.split(' ')
    if columns is None:
        return d1
    if isinstance(columns, list):
        for r in columns:
            if r in d2:
                d1[r] = d2[r]
    elif isinstance(columns, dict):
        for k, v in columns.items():
            d1[v] = d2[k]
    return d1


import types


def tramp(gen, *args, **kwargs):
    g = gen(*args, **kwargs)
    while isinstance(g, types.GeneratorType):
        g = g.next()
    return g


import inspect


def is_iter(item):
    if isinstance(item, list):
        return True
    if inspect.isgenerator(item):
        return True


def first_or_default(generator):
    for r in generator:
        return r
    return None


def query(data, key, default=None):
    if data is None:
        return key
    if isinstance(data, dict):
        if is_str(key) and key.startswith('[') and key.endswith(']'):
            key = key[1:-1]
            if key == '_':
                key = default
            if key in data:
                return data[key]
            else:
                return None
    return key


def get_value(data, key):
    if key in ['', None]:
        return data
    if isinstance(data, dict):
        return data.get(key, None)
    else:
        if hasattr(data, key):
            return getattr(data, key)
        return None


def set_value(data, key, value):
    if key in ['', None]:
        return data
    if isinstance(data, dict):
        data[key] = value
    else:
        setattr(data, key, value)
    return data


def has(data, key):
    if isinstance(data, dict):
        return key in data
    else:
        return key in data.__dict__


def del_value(data, key):
    if key in ['', None]:
        return
    if isinstance(data, dict):
        del data[key]
    else:
        del data.__dict__[key]


def get_variance(n_list):
    sum1 = 0.0
    sum2 = 0.0
    N = len(n_list)
    for i in range(N):
        sum1 += n_list[i]
        sum2 += n_list[i] ** 2
    mean = sum1 / N
    var = sum2 / N - mean ** 2
    return var


def find_any(iter, filter):
    for r in iter:
        if filter(r):
            return True
    return False


def get_index(iter, filter):
    for r in range(len(iter)):
        if filter(iter[r]):
            return r
    return -1


def get_indexs(iter, filter):
    res = []
    for r in range(len(iter)):
        if filter(iter[r]):
            res.append(r)
    return res


def cross(a, gene_func, env):
    for r1 in a:
        r1 = dict.copy(r1)
        for r2 in gene_func(r1, env):
            for key in r2:
                r1[key] = r2[key]
            yield dict.copy(r1)


def mix(g1, g2):
    while True:
        t1 = g1.next()
        if t1 is None:
            pass
        else:
            yield t1
        t2 = g2.next()
        if t2 is None:
            pass
        else:
            yield t2
        if t1 is None and t2 is None:
            return


def cross_array(a, b, func):
    for i in a:
        for j in b:
            yield func(i, j)


def merge_all(a, b):
    while True:
        t1 = a.__next__()
        if t1 is None:
            return
        t2 = b.__next__()
        if t2 is not None:
            for t in t2:
                t1[t] = t2[t]
        yield t1


def append(a, b):
    for r in a:
        yield r
    for r in b:
        yield r


def get_type_name(obj):
    import inspect
    if inspect.isclass(obj):
        s = str(obj)
    else:
        s = str(obj.__class__)
    p = s.find('.')
    r = s[p + 1:].split('\'')[0]
    r = r.replace('tools.', '')
    return r


def copy(x):
    if hasattr(x, 'copy'):
        return x.copy()
    return x


class EObject(object):
    '''
        empty class, which mark a class to be a dict.
    '''
    def __getstate__(self):
        """Return state values to be pickled."""
        dic = {}
        for k, v in self.__dict__.items():
            if k.startswith('__'):
                continue
            dic[k] = v
        return dic

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        for k, v in state.items():
            self.__setattr__(k, v)


def get_range(range, env=None):
    def get(key):
        return query(env, key)

    buf = [r for r in range.split(':')]
    start = 0
    end = interval = 1
    if len(buf) > 2:
        interval = get_num(get(buf[2]))
    if len(buf) > 1:
        end = get_num(get(buf[1]))
    if len(buf)>0:
        start = get_num(get(buf[0]))
    return start, end, interval


def convert_to_builtin_type(obj):
    return {key: value for key, value in obj.__dict__.items() if
            isinstance(value, (str, int, float, list, dict, tuple, EObject) or value is None)}


def dict_to_poco_type(obj):
    if isinstance(obj, dict):
        result = EObject()
        for key in obj:
            v = obj[key]
            setattr(result, key, dict_to_poco_type(v))
        return result
    elif isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = dict_to_poco_type(obj[i])
    return obj


def dict_copy_poco(obj, dic):
    for key, value in obj.__dict__.items():
        if key in dic:
            value = dic[key]
            if isinstance(value, (int, float)) or is_str(value):
                setattr(obj, key, value)


def convert_dict(obj):
    if not isinstance(obj, (int, float, list, dict, tuple, EObject)) and not is_str(obj):
        return None
    if isinstance(obj, EObject):
        d = {}
        obj_type = type(obj)
        typename = get_type_name(obj)
        default = obj_type().__dict__
        if typename == 'ETLTask':
            d['tools'] = convert_dict(obj.tools)
        else:
            for key, value in obj.__dict__.items():
                if value == default.get(key, None):
                    continue
                if key.startswith('_'):
                    continue
                p = convert_dict(value)
                if p is not None:
                    d[key] = p
        d['Type'] = typename
        return d

    elif isinstance(obj, list):
        return [convert_dict(r) for r in obj]
    elif isinstance(obj, dict):
        return {key: convert_dict(value) for key, value in obj.items()}
    return obj


def group_by_mount(generator, group_count=10):
    tasks = []
    task_id = 0
    if isinstance(generator, list):
        generator = (r for r in generator)
    while True:
        task = next(generator, None)
        if task is None:
            yield tasks[:]
            return
        tasks.append(task)
        if len(tasks) >= group_count:
            yield tasks[:]
            task_id = task_id + 1
            tasks = []
