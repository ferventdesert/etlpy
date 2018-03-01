# coding=utf-8
import codecs
import time, os, sys

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
import traceback
from multiprocessing.managers import BaseManager
import os, sys
from etlpy.extends import PY2, is_in_ipynb, convert_dict, group_by_mount
from etlpy.multi_yielder import NETWORK_MODE
from etlpy.tools import Project, parallel_map, ex_generate
import json
import pickle,dill
authkey = "etlpy".encode('utf-8')
timeout = 1
rpc_port = 28998

if PY2:
    from Queue import Queue
else:
    from queue import Queue

from flask import request, jsonify
from flask import Flask

finished_job = []
dispatched_job = []
app = Flask(__name__)


class Master:
    def __init__(self):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()

    def get_dispatched_job_queue(self):
        return self.dispatched_job_queue

    def get_finished_job_queue(self):
        return self.finished_job_queue

    def server_init(self, port=None):
        if port is None:
            port = rpc_port
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue', callable=self.get_dispatched_job_queue)
        BaseManager.register('get_finished_job_queue', callable=self.get_finished_job_queue)
        print('job queue port is %d' % port)
        # 监听端口和启动服务
        manager.start()
        print('server started')
        if is_in_ipynb():
            print('exec in ipython notebook')

    def start_server(self, port=6007):
        self.server_init()
        app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)

    def start_project(self, project, job_name, _rpc_port=None):
        self.server_init(_rpc_port)
        module = self.project.env[job_name]
        dispatched_jobs = self.manager.get_dispatched_job_queue()
        try:
            while True:
                while True:
                    for job in module.pl_geneator():
                        dispatched_jobs.put(job)
                if not extends.is_ipynb:
                    key = input('press any key to repeat,c to cancel')
                    if key == 'c':
                        break
            manager.shutdown()
        except Exception as e:
            import traceback
            traceback.print_exc()
            print('manager has shutdown')
            manager.shutdown()


@app.route('/task/query/<method>', methods=['GET'])
def query_task(method):
    job_queue = manager.get_finished_job_queue()
    while not job_queue.empty():
        job = job_queue.get(60)
        finished_job.append(job)
    result = {"remain": manager.get_dispatched_job_queue().qsize()}
    if method == 'finished':
        result[method] = finished_job
    elif method == 'dispatched':
        result[method] = dispatched_job
    elif method == 'clean':
        for r in dispatched_job:
            dispatched_job.remove(r)
    result = jsonify(**result)
    return result


@app.route('/task/insert', methods=['POST'])
def insert_task():
    job = request.json  # have no the json data?
    dispatched_job_queue = manager.get_dispatched_job_queue()
    result = {"status": "success", "remain": dispatched_job_queue.qsize()}
    # for r in manager.get_finished_job_queue():
    #     if r['id'] == job['id'] and r['name'] == job['name']:
    #         result['status']='failed'
    #         result['status'] = 'repeat,ignore'
    #         print('task %s, id %s finished, so skip...'%(r['name'],r['id']))
    #         return jsonify(**result)
    dispatched_job_queue.put(job)
    dispatched_job.append(job)
    result = jsonify(**result)
    return result


# 问题
# 1. 传送任务一定要带project吗？ 传，但不希望每次都传，分组可以搞大一点

class Slave:
    def __init__(self):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()

    def start(self, execute=True, server_ip='127.0.0.1', port=rpc_port):
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue')
        BaseManager.register('get_finished_job_queue')
        server = server_ip
        print('Connect to server %s...' % server)
        manager = BaseManager(address=(server, port), authkey=authkey)
        manager.connect()
        # 使用上面注册的方法获取队列
        dispatched_jobs = manager.get_dispatched_job_queue()
        finished_jobs = manager.get_finished_job_queue()

        while True:
            if dispatched_jobs.empty():
                print('task finished, delay 20s')
                time.sleep(20)
                continue
            job = dispatched_jobs.get(timeout=timeout)
            project, name, tasks, job_id, env = job['proj'], job['name'], job['tasks'], job['id'], job['env']
            print('Run job: %s ' % job_id)

            proj = dill.loads(codecs.decode(project.encode(), "base64"))
            etl_task = proj.env[name]
            total_count = 0
            task_result = dict(name=name, job_id=job_id)
            mapper, reducer, tool = parallel_map(etl_task.tools, env)
            try:
                generator = ex_generate(reducer, tasks, env=env)
                for item in generator:
                    total_count += 1
                task_result['count'] = total_count
                task_result['message'] = 'success'
            except Exception as e:
                task_result['message'] = str(e)
                traceback.print_exc()
            print('finish job,id %s, count %s' % (job_id, total_count))
            finished_jobs.put(task_result)


## RPC控制端口和共享端口不是一个

if __name__ == '__main__':
    manager = BaseManager(address=('0.0.0.0', rpc_port), authkey=authkey)
    ip = '127.0.0.1'  # '10.101.167.107'

    mode = 'client'
    argv = sys.argv
    port = 6067
    if len(argv) > 1:
        mode = argv[1]
    if mode == 'server':
        if len(argv) > 2:
            port = int(argv[2])
        master = Master()
        master.start_server(port)
    else:
        port= rpc_port
        if len(argv) > 2:
            ip = argv[2]
        if len(argv) > 3:
            port = int(argv[3])
        slave = Slave()
        slave.start(True, ip, port)
