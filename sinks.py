#coding: utf-8


class DataSink(object):
    name = 'Default sink'

    def emit(self, timestamp, key, value):
        raise NotImplemented


class PrinterSink(DataSink):
    def emit(self, timestamp, key, value):
        print u'%s %s %i' % (key, value, timestamp)


