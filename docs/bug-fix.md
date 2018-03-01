

## ok
生成器，range(1,1,1),返回默认值 ok

增加mix方法 ok


def cross(a, gene_func):
    for r1 in a:
        r1=dict.copy(r1);
        for r2 in gene_func(r1):
            for key in r2:
                r1[key] = r2[key]
                yield dict.copy(r1);

因为改了一下缩进，让yield往前退了一步，调试超过半个小时。。。

## 2016年10月08日

增加RotateTF ok,

新建链家爬虫

### 需要解决

并行执行器在子流层面的问题？ 子流内部不做任何并行化 OK

###

判断是否是字符串,这个东西花了多长的时间才暴露出来!!!  merge_query!!!

## 生成器 ok

如果生成器返回的是字典数组，后期的每个操作都会改变这个字典的值，导致无法重复执行，显然要copy一下

一旦引擎发生改变，要修改的代码就很多。。。所以一定要稳定



## 多线程问题 ok!

如何多线程，多机，并行，多进程等等。如果一次设计，多次运行的话，是不可能提前知道这些信息的。需要在运行时动态判定

支持动态判定，同时也支持在代码中实现，可以通过外部配置强制实现。

动态判定可能会有bug,之后考虑删除

因此，pl还是叫pl. query, 还有执行方式和线程数。 但这样参数就比较多了。 只包含是否回流数据回来的逻辑。

多线程，多进程可以一体化。产生多少个线程可以配置。传一个配置环境进去。

多进程无法传递生成器，但可以传递种子

## 多机分布式 ok!

## 反爬虫机制 

代理+ 延时。此时不能支持并行。提供代理池，之后每次访问，向前推一个。 

全局访问器。 延时管理。

IP来源？买。。。 传一个数组进去  ok





## 环境传递 ok 

链式操作，但实际上分为明显的编译时和运行时。编译时的参数如何传递给运行时呢

加入一个环境，只有当编译时才知道参数，比如column。 类似的，还能加入enabled,execute这些变量。当然都是隐含的。

最后一个问题，环境是self.env么？最好可以。否则会很麻烦。

缺点呢？ 不太会有同时编译，但会有同时执行。那时已经编译完了

结论，不用self.env，而是用临时变量env


## 要不要做下去 ？ ok

做！沉没成本，但是已经花了这么多精力，放弃太过可惜。
极端的linq... 一定要全部链路都这么设计么？

## 增加缓存  ok!

否则每次执行的速度特别慢。 

- 要么增加全局缓存
- 要么就是爬虫的缓存 爬虫缓存比较简单合理

考虑写文件，把url和参数对应的值存起来，除了web，其他也没什么太过耗费的。如果需要可以加

## UI设计

- 自己开发
自动代码提示，参数提示，功能帮助，语法高亮，web化，所见即所得
- anaconda
需增加缓存，体验不佳，但大家都比较清楚。 
先用anaconda吧

## enable的问题

不要做的那么反人类，不需要就删掉，没那么多废话。

需要把流拆开，然后组装，没必要把流程搞的那么长，流式语言也有设计模式。

可以在任意位置插入调试，之后的流被截断，这样就不用反复地注释了  ok
## 反爬虫逻辑

需要一个合适的机制，设置一个爬虫所需的环境 基本ok

另外，流式语言一定要面向爬虫么？

format体验不好，一定要{} 算了
```

## 去掉不必要的库依赖 ok

只在必要的时候再去安装相应的库s
flask
ipy_notebook
ipython
pyquery
simplejson : 如果只是dump的话，为何不用原生的json?
pandas: 

    def process_req(self, args):
        if self.delay != 0:
            time.sleep(self.delay)
        headers = self.headers
        if headers not in ({}, ''):
            if is_str(headers):
                headers = para_to_dict(headers, '\n', ',')
            if self.agent:
                headers['User-Agent'] = random.choice(USER_AGENTS)
            args['headers'] = headers
        if self.proxy is not None and len(self.proxy) > 0:
            l = len(self.proxy) - 1
            if self.allow_local == True:
                l += 1
            index = random.randint(0, l)
            if index < len(self.proxy):
                proxy = self.proxy[index]
                args['proxies'] = {'http': proxy}
                
        ```        
        
        
        def proxy(port=8000):
    from http_proxy import LoggingProxyHTTPHandler
    import BaseHTTPServer
    server_address = ('', port)
    print('start proxy')
    httpd = BaseHTTPServer.HTTPServer(server_address, LoggingProxyHTTPHandler)
    
    httpd.serve_forever()
    
    
    
# 避免频繁地生成生成器 ok

# 2017年10月17日

改进子流 ok
需要提供本地的并行存取方案

## 错误重试 

目前还没有错误重试功能，应当记录

对执行的理解


# 2017年10月31日

list().html().tree()特别繁琐，有可能简化掉吗？

not作为where的前缀，实现求反。


## etlpy的推广规划

1. hawk的github上，启动页面。xxx
2. 博客园
3. ATA
4. 微信公众号 

两篇： 纯技术向，设计思路向。  

优先级： 低，收集反馈， 12月份完成即可
估计github star量； 500,使用人数10K
文档： 目前完成度80%，中英文同时提供（笑帮忙翻译）

将etlpy翻译为pandas文法，速度会大幅度提升
用where语句解决逻辑问题

extend 可以通过脚本，将所需的脚本添加到后面去，相当于流的流

针对特征处理的算子，onehot,交叉组合，优选逻辑

生成sql

tensor_provider, 发ATA上，会有很多人看

对join的优化

到处设计异常，看看程序能否正常运行，好的程序必须对异常做出处理

设计查看任务进展的webui

rangetf... cross的，其实在后面叠加一个list就行了，若是字典再加drill。不过缺点是更难理解了。另外生成器后面默认也是能跟PL的





