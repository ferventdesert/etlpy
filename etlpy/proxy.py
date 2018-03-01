# coding=utf-8
import json

# if PY2:
#     import StringIO
#     from BaseHTTPServer import BaseHTTPRequestHandler
# else:
#     from http.server import BaseHTTPRequestHandler, HTTPServer
import gzip
import requests
import time

from etlpy.spider import get_encoding

proxy_url='http://123.207.35.36:5010'

def get_proxy():
    return requests.get(proxy_url+"/get/").content

def get_proxy_all():
    url= requests.get(proxy_url+"/get_all/").json()
    return url

def delete_proxy(proxy):
    requests.get(proxy_url+"/delete/?proxy={}".format(proxy))



USER_AGENTS = [
	"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
	"Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
	"Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
	"Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
	"Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
	"Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
	"Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
	"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
	"Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
]






"""
usage 'pinhole port host [newport]'

Pinhole forwards the port to the host specified.
The optional newport parameter may be used to
redirect to a different port.

eg. pinhole 80 webserver
    Forward all incoming WWW sessions to webserver.

    pinhole 23 localhost 2323
    Forward all telnet sessions to port 2323 on localhost.
"""

import sys
from socket import *
from threading import Thread
import time

LOGGING = 1

def log(s):
    if LOGGING:
        print
        '%s:%s' % (time.ctime(), s)
        sys.stdout.flush()


class PipeThread(Thread):
    pipes = []

    def __init__(self, source, sink):
        Thread.__init__(self)
        self.source = source
        self.sink = sink
        log('Creating new pipe thread  %s ( %s -> %s )' % \
            (self, source.getpeername(), sink.getpeername()))
        PipeThread.pipes.append(self)
        log('%s pipes active' % len(PipeThread.pipes))

    def run(self):
        while 1:
            try:
                data = self.source.recv(1024)
                if not data: break
                self.sink.send(data)
            except:
                break

        log('%s terminating' % self)
        PipeThread.pipes.remove(self)
        log('%s pipes active' % len(PipeThread.pipes))


class Pinhole(Thread):
    def __init__(self, port, newhost, newport):
        Thread.__init__(self)
        log('Redirecting: localhost:%s -> %s:%s' % (port, newhost, newport))
        self.newhost = newhost
        self.newport = newport
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind(('', port))
        self.sock.listen(5)

    def run(self):
        while 1:
            newsock, address = self.sock.accept()
            log('Creating new session for %s %s ' % address)
            fwd = socket(AF_INET, SOCK_STREAM)
            fwd.connect((self.newhost, self.newport))
            PipeThread(newsock, fwd).start()
            PipeThread(fwd, newsock).start()


if __name__ == '__main__':

    print('Starting Pinhole')

    import sys

    sys.stdout = open('pinhole.log', 'w')

    if len(sys.argv) > 1:
        port = newport = int(sys.argv[1])
        newhost = sys.argv[2]
        if len(sys.argv) == 4: newport = int(sys.argv[3])
        Pinhole(port, newhost, newport).start()
    else:
        Pinhole(80, 'hydrogen', 80).start()
        Pinhole(23, 'hydrogen', 23).start()



