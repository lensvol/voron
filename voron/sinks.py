#coding: utf-8

import asyncore
import socket
import time

class DataSink(object):
    name = 'Default sink'

    def __init__(self, prefix=None):
        if prefix:
            self.prefix = '%s.' % prefix
        else:
            self.prefix = ''

    def emit(self, metric_type, key, value, timestamp=None, hints=None):
        raise NotImplemented


class PrinterSink(DataSink):
    name = 'stdout printer'

    def emit(self, metric_type, key, value,  timestamp=None, hints=None):
        print u'%s%s %s %i' % (self.prefix, key, value, timestamp or time.time())


class GraphiteSink(DataSink, asyncore.dispatcher):
    name = 'GraphiteSink'

    def connect_to_graphite(self):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((self.host, self.port))

    def __init__(self, *args, **kwargs):        
        asyncore.dispatcher.__init__(self)
        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 2003)
        self.buffer = ''
        self.connect_to_graphite()
        super(GraphiteSink, self).__init__(*args, **kwargs)

    def handle_connect(self):
        print 'CONNECT'

    def handle_write(self):
        sent = self.send(self.buffer)
        print 'SENT: %s' % self.buffer[:sent]
        self.buffer = self.buffer[sent:]

    def handle_close(self):
        print 'CLOSED :-( Reconnecting...'
        self.close()
        self.connect_to_graphite()

    def writable(self):
        return (len(self.buffer) >= 8)

    def emit(self, metric_type, key, value, timestamp=None, hints=None):
        line = '%s%s %s %i\n' % (self.prefix, key, value, timestamp or time.time())
        self.buffer += line
        print 'EMIT: %s' % line

 
class StatsiteSink(DataSink):
    name = 'statsd sink'

    def __init__(self, *args, **kwargs):
        super(StatsiteSink, self).__init__(*args, **kwargs)
        self.host = kwargs.pop('host', '127.0.0.1')
        self.port = kwargs.pop('port', 8125)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def emit(self, metric_type, key, value, **kwargs):
        codes = {
            'gauge': 'g',
            'timing': 'ms',
            'counter': 'c',
            'set': 's'
        }
        assert metric_type in codes, 'Unknown metric type!'

        self.sock.sendto('%s%s:%s|%s' % (self.prefix, key, value, codes[metric_type]),
                         (self.host, self.port))
