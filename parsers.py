#coding: utf-8

import re
import time
import datetime

class LineParser(object):
    name = None

    def __init__(self, sink):
        assert sink, 'Sink should be an object!'
        self.sink = sink

    def parse(self, line):
        raise NotImplemented

class EchoParser(LineParser):
    name = 'Echo'

    def parse(self, line):
        stamp = time.time()
        key = 'sample_line'
        value = line

        self.sink.emit(stamp, key, value)
        return True


class CeleryParser(LineParser):
    name = 'Celery'

    def __init__(self, *args, **kwargs):
        super(CeleryParser, self).__init__(*args, **kwargs)
        
        self.got_task_re = re.compile(r'\[(.+):.+\] Got task from broker: ([\w_\d]+)\[(.+)\]')
        self.succeeded_re = re.compile(r'\[(.+):.+\] Task ([\w_\d]+)\[(.+)\] succeeded in ([0-9\.]+)s: .+')

    def parse(self, line):
        to_date = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S,%f')

        m = self.got_task_re.match(line)
        if m:
            timestamp = time.mktime(to_date(m.group(1)).timetuple())
            task_name = m.group(2)
            task_id = m.group(3)

            self.sink.emit(timestamp, 'celery.tasks.%s.started' % task_name, '1')
            return True

        m = self.succeeded_re.match(line)
        if m:
            timestamp = time.mktime(to_date(m.group(1)).timetuple())
            task_name = m.group(2)
            task_id = m.group(3)
            duration = m.group(4)

            self.sink.emit(timestamp, 'celery.tasks.%s.duration' % task_name, duration)
            return True

        return False
