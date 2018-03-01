# -*- coding: utf-8 -*-
# coding=utf-8
import codecs
import logging
import multiprocessing
import os

from etlpy.extends import Queue,Empty

NORMAL_MODE= 0
THREAD_MODE = 1
PROCESS_MODE =2
ASYNC_MODE = 'async'
NETWORK_MODE =3
DEFAULT_WORKER_NUM=20

open = codecs.open


class Stop(Exception):
    "Exception raised by Queue.get(block=0)/get_nowait()."
    pass


class Yielder(object):
    def __init__(self, dispose):
        self.dispose = dispose

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()


def safe_queue_get(queue, is_stop_func=None, timeout=2):
    while True:
        if is_stop_func is not None and is_stop_func():
            return Stop
        try:
            data = queue.get(timeout=timeout)
            return data
        except Exception as e:
            continue


def safe_queue_put(queue, item, is_stop_func=None, timeout=2):
    while True:
        if is_stop_func is not None and is_stop_func():
            return Stop
        try:
            queue.put(item, timeout=timeout)
            return item
        except Exception as e:
            continue


def multi_yield(customer_func, mode=THREAD_MODE, worker_count=1, generator=None, queue_size=10):
    workers = []

    def is_alive(process):
        if mode == PROCESS_MODE:
            return process.is_alive()
        elif mode == THREAD_MODE:
            return process.isAlive()
        return True

    class Stop_Wrapper():
        def __init__(self):
            self.stop_flag = False
            self.workers=[]

        def is_stop(self):
            return self.stop_flag

        def stop(self):
            self.stop_flag = True
            for process in self.workers:
                if isinstance(process,multiprocessing.Process):
                    process.terminate()

    stop_wrapper = Stop_Wrapper()

    def _boss(task_generator, task_queue, worker_count):
        for task in task_generator:
            item = safe_queue_put(task_queue, task, stop_wrapper.is_stop)
            if item is Stop:
                return
        for i in range(worker_count):
            task_queue.put(Empty)

    def _worker(task_queue, result_queue, gene_func):
        import time
        try:
            def generator():
                while not stop_wrapper.is_stop():
                    if task_queue.empty():
                        time.sleep(0.01)
                        continue
                    task = safe_queue_get(task_queue, stop_wrapper.is_stop)
                    if task == Empty:
                        result_queue.put(Empty)
                        break
                    if task == Stop:
                        break
                    yield task
            for item in gene_func(generator()):
                item = safe_queue_put(result_queue, item, stop_wrapper.is_stop)
                if item == Stop:
                    break
        except Exception as e:
            logging.exception(e)

    def factory(func, args=None, name='task'):
        if args is None:
            args = ()
        if mode == PROCESS_MODE:
            return multiprocessing.Process(name=name, target=func, args=args)
        if mode == THREAD_MODE:
            import threading
            t = threading.Thread(name=name, target=func, args=args)
            t.daemon = True
            return t
        if mode == ASYNC_MODE:
            import gevent
            return gevent.spawn(func, *args)

    def queue_factory(size):
        if mode == PROCESS_MODE:
            return multiprocessing.Queue(size)
        elif mode == THREAD_MODE:
            return Queue(size)
        elif mode == ASYNC_MODE:
            from gevent import queue
            return queue.Queue(size)

    def should_stop():
        if not any([r for r in workers if is_alive(r)]) and result_queue.empty():
            return True
        return stop_wrapper.is_stop()

    if mode is None or mode == NORMAL_MODE:
        for item in customer_func(generator):
                yield item
        return
    with Yielder(stop_wrapper.stop):
        result_queue = queue_factory(queue_size)
        task_queue = queue_factory(queue_size)

        main = factory(_boss, args=(generator, task_queue, worker_count), name='_boss')
        for process_id in range(0, worker_count):
            name = 'worker_%s' % (process_id)
            p = factory(_worker, args=(task_queue, result_queue, customer_func), name=name)
            workers.append(p)
        main.start()
        stop_wrapper.workers = workers[:]
        stop_wrapper.workers.append(main)
        for r in workers:
            r.start()
        count = 0
        while not should_stop():
            data = safe_queue_get(result_queue, should_stop)
            if data is Empty:
                count += 1
                if count == worker_count:
                    break
                continue
            if data is Stop:
                break
            else:
                yield data


def get_split(datas, count, index):
    l = len(datas)
    assert index < count
    if count > l:
        count = l
    seg = l / count
    end = l if index == count - 1 else seg * (index + 1)
    data = datas[seg * index:  end]
    return data


def exec_cmd(exec_str):
    os.system(exec_str)


