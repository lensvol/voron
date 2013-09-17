#coding: utf-8

import os
import asyncore
import codecs
import pyinotify

from optparse import OptionParser

from parsers import CeleryParser, EchoParser, GunicornParser, NginxParser
from sinks import PrinterSink, GraphiteSink


BUFFER_SIZE = 4096


class ParserNotFound(Exception):
    pass


class FSEventHandler(pyinotify.ProcessEvent):
    def my_init(self, mapping):
        self.mapping = mapping
        self.positions = dict((fn, 0) for fn in mapping.keys())
        self.fileobjs = {}

        for fn in mapping.keys():
            self.open_file(fn)

    def consume_lines(self, fn):
        fp = self.fileobjs[fn]
        handler = self.mapping[fn]

        data = fp.read(BUFFER_SIZE)
        lines = data.split('\n')
        for line in lines[:-1]:
            handler.parse(line)
        fp.seek(len(lines[-1]), 1)

    def open_file(self, fn):
        if not fn in self.fileobjs:
            self.fileobjs[fn] = fp = codecs.open(fn, 'rb', 'utf-8')
            pos = self.positions[fn]
            fp.seek(pos)
        return self.fileobjs[fn]

    def process_IN_DELETE(self, event):
        if event.pathname in self.mapping:
            fp = self.fileobjs.pop(event.pathname)
            fp.close()

        self.positions[fn] = 0

    def process_IN_CREATE(self, event):
        fn = event.pathname
        if fn in self.mapping:
            self.open_file(fn)
            self.consume_lines(fn)

    def process_IN_MODIFY(self, event):
        if event.pathname in self.mapping:
            self.consume_lines(event.pathname)

if __name__ == '__main__':
    opt_parser = OptionParser()
    opt_parser.add_option("-P", "--playback", dest="just_playback",
                          help="Parse specified files and exit.",
                          action="store_true", default=False)
    opt_parser.add_option("-s", "--sink", dest="sink",
                          help="Sink for processed time series data.",
                          action="store", default="print")
    (options, args) = opt_parser.parse_args()

    sinks = {
        'print': PrinterSink,
        'graphite': GraphiteSink
    }
    parsers = {
        'echo': EchoParser,
        'celery': CeleryParser,
        'gunicorn': GunicornParser,
        'nginx': NginxParser
    }
    
    default_sink = sinks.get(options.sink)
    if not default_sink:
        print '[ERROR] unknown sink: %s' % options.sink
        sys.exit(-1)

    file_mapping = {}
    for mapping in args:
        name, fn = mapping.split(':')
        if not name in parsers:
            raise ParserNotFound('Unknown parser: %s' % name)
        file_mapping[os.path.abspath(fn)] = parsers[name](default_sink())

    if options.just_playback:
        for fn, parser in file_mapping.items():
            with codecs.open(fn, 'rt', 'utf-8') as fp:
                while True:
                    line = fp.readline()
                    if not line:
                        break
                    parser.parse(line.strip())
                    asyncore.loop(1, False, None, 1)
    else:
        wm = pyinotify.WatchManager()
        handler = FSEventHandler(mapping=file_mapping)
        notifier = pyinotify.Notifier(wm, default_proc_fun=handler)

        for fn, parser in file_mapping.items():
            print '[%s] Watching %s...' % (parser.name, fn)
            wm.add_watch(os.path.dirname(fn), 
                         pyinotify.IN_OPEN | pyinotify.IN_CREATE | 
                         pyinotify.IN_DELETE | pyinotify.IN_MODIFY)

        notifier.loop()
