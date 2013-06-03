#coding: utf-8

import asyncore, socket

class DataSink(object):
    name = 'Default sink'

    def emit(self, timestamp, key, value):
        raise NotImplemented


class PrinterSink(DataSink):
    def emit(self, timestamp, key, value):
        print u'%s %s %i' % (key, value, timestamp)


class GraphiteSink(DataSink, asyncore.dispatcher):
    def connect_to_graphite(self):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((self.host, self.port))

    def __init__(self, host=None, port=2003):
        asyncore.dispatcher.__init__(self)
        self.host = host or 'localhost'
        self.port = port or 2003
        self.buffer = ''
        self.connect_to_graphite()
        print self.host, self.port

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

    def emit(self, timestamp, key, value):
        line = '%s %s %i\n' % (key, value, timestamp)
        self.buffer += line
        print 'EMIT: %s' % line
