# coding=utf-8
import inspect

from etlpy.extends import is_str, concat, to_list
from etlpy.tools import ETLTool, Transformer, Executor, Generator, SubBase, WebTF, Filter, Project, MongoDBConnector, \
    ETLTask

from etlpy import tools, extends
__base_type = [ETLTool, Filter, WebTF, SubBase, Generator, Executor, Transformer]
__ignore_paras = ['one_input', 'multi', 'column', 'p']

tool_dict = {}
proj = Project()


def set_level(level):
    extends.set_level(level)



def __get_etl(dic):
    for name, tool_type in tools.__dict__.items():
        if not inspect.isclass(tool_type):
            continue
        if not issubclass(tool_type, ETLTool):
            continue
        if tool_type in __base_type:
            continue
        dic[name] = tool_type


__get_etl(tool_dict)

#
# def process_req(self, args):
#     if self.delay != 0:
#         time.sleep(self.delay)
#     headers = self.headers
#     if headers not in ({}, ''):
#         if is_str(headers):
#             headers = para_to_dict(headers, '\n', ',')
#         if self.agent:
#             headers['User-Agent'] = random.choice(USER_AGENTS)
#         args['headers'] = headers
#     if self.proxy is not None and len(self.proxy) > 0:
#         l = len(self.proxy) - 1
#         if self.allow_local == True:
#             l += 1
#         index = random.randint(0, l)
#         if index < len(self.proxy):
#             proxy = self.proxy[index]
#             args['proxies'] = {'http': proxy}
#
#


def html(text):
    from IPython.core.display import HTML, display
    display(HTML(text))


def get_default_connector():
    mongo = MongoDBConnector()
    mongo.connect_str = 'mongodb://10.244.0.112'
    mongo.db = 'ant_temp'
    proj.env['mongo'] = mongo
    return mongo


# def proxy(port=8000, LoggingProxyHTTPHandler=):
#     from http_proxy import LoggingProxyHTTPHandler
#     import BaseHTTPServer
#     server_address = ('', port)
#     print('start proxy')
#     httpd = BaseHTTPServer.HTTPServer(server_address, LoggingProxyHTTPHandler)
#     httpd.serve_forever()


def task(name='etl'):
    _task = ETLTask()
    _task._proj = proj
    if name in proj.env:
        name +='_'+str(len(proj.env.keys()))
    _task.name = name
    proj.env[name] = _task

    def attr_filler(attr):
        if attr.startswith('_'):
            return True
        if attr in __ignore_paras:
            return True
        return False

    def set_attr(val, dic):
        default = type(val)().__dict__
        for key in val.__dict__:
            if key.startswith('_'):
                continue
            dv = default[key]
            value = dic.get(key, dv)
            if key == 'p' and is_str(value) and value == '':
                continue
            if value is not None:
                setattr(val, key, value)

    def _rename(module):
        repl = {'TF': '', 'Regex': 're', 'Parallel': 'pl', 'Remove': 'rm', 'Move': 'mv', 'Copy': 'cp'}
        for k, v in repl.items():
            module = module.replace(k, v)
        return module.lower()

    dynaimc_method = '''def __%s(p='',%s):
        import etlpy.tools as etl
        new_tool=etl.%s()
        new_tool._proj=proj
        _task.tools.append(new_tool)
        set_attr(new_tool,locals())
        return _task
    '''

    def merge_func(k, v):
        if is_str(v):
            v = "'%s'" % (v)
        yield '%s=%s' % (k, v)

    def etl_help():
        import inspect
        for k, v in _task.__dict__.items():
            if inspect.isfunction(v):
                doc = v.__doc__
                if doc is None:
                    doc = 'doc is invalid'
                else:
                    doc = doc.split('\n')
                    for d in doc:
                        d = d.strip()
                        if d != '':
                            doc = d
                            break
                print(k + ':\t' + doc)

    for name, tool_type in tools.__dict__.items():
        if not inspect.isclass(tool_type):
            continue
        if not issubclass(tool_type, ETLTool):
            continue
        if tool_type in __base_type:
            continue
        tool = tool_type()
        paras = to_list(concat((merge_func(k, v) for k, v in tool.__dict__.items() if not attr_filler(k))))
        paras.sort()
        paras = ','.join(paras)
        new_name = _rename(name)
        method_str = dynaimc_method % (new_name, paras, name)
        locals()['proj'] = proj
        exec(method_str, locals())
        func = locals()['__' + new_name]
        func.__doc__ = tool.__doc__
        setattr(_task, new_name, func)
    setattr(_task, 'help', etl_help)
    return _task


if __name__ == '__main__':
    pass
