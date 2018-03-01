import inspect
import random
import types

from etlpy.extends import EObject, query
from etlpy.proxy import USER_AGENTS


class Param(EObject):
    def __init__(self, value=None, **kwargs):
        super(Param, self).__init__()
        if value is not None:
            self.value = value
        else:
            self.value= kwargs

    def __str__(self):
        if isinstance(self.value, dict):
            return 'Param{' +' , '.join('%s:%s'%(k,v) for k,v in self.value.items()) + '}'
        return  'Param{'+str(type(self.value)) + '}'


    def __setitem__(self, instance, value):
        assert isinstance(self.value, dict)
        self.value[instance]=value

    def get(self, data,col):
        values = self.value.copy()
        if isinstance(values, dict):
            for k, v in values.items():
                if issubclass(type(v), Param):
                    values[k] = v.get(data,col)
                if inspect.isfunction(v):
                    values[k] = v(data)
        elif inspect.isfunction(values) or isinstance(values, types.LambdaType):
            values= values()
        return values

    def merge_all(self, item):
        assert issubclass(type(item),Param) or isinstance(item,dict)
        dic3 = self.value.copy()
        value = item if isinstance(item,dict) else item.value
        for k, v in value.items():
            dic3[k] = v
        return Param(dic3)

    def merge(self, key, item):
        dic3 = self.value.copy()
        if key in dic3:
            dic3[key] = dic3[key].merge_all(item)
        else:
            dic3[key]= item
        return Param(dic3)

    def copy(self):
        return Param(self.value.copy())


class ExpParam(Param):
    def get(self, data,col):
        if inspect.isfunction(self.value):
            return self.value(data)
        return query(data, self.value,col)


class RandomParam(Param):
    def get(self, data,col):
        return random.choice(self.value)


request_param = Param({'url': ExpParam('[_]'), 'headers': Param({'user_agents': USER_AGENTS[-2]})})

if __name__ == '__main__':
    print(request_param.get({}))