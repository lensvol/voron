#coding: utf-8

import apachelog
import datetime
import hashlib
import logging
import re
import time


class LineParser(object):
    name = None

    def __init__(self, sink):
        assert sink, 'Sink should be an object!'
        self.sink = sink

    def parse(self, line):
        raise NotImplemented


class HashParser(LineParser):
    name = 'Echo'

    def parse(self, line):
        m = hashlib.md5()
        m.update(line)
        key = m.hexdigest()
        stamp = time.time()        
        value = 1

        self.sink.emit('timing', key, value, timestamp=stamp)
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

            self.sink.emit('counter', 'celery.tasks.%s.started' % task_name, '1', 
                           timestamp=timestamp)
            return True

        m = self.succeeded_re.match(line)
        if m:
            timestamp = time.mktime(to_date(m.group(1)).timetuple())
            task_name = m.group(2)
            task_id = m.group(3)
            duration = m.group(4)

            self.sink.emit('gauge', 'celery.tasks.%s.duration' % task_name, duration,
                           timestamp=timestamp)
            return True

        return False


class CommonLogFormatParser(LineParser):
    u'''
    Обработка записей журнала доступа веб-сервера, отвечающих стандарту
    Common Log Format.
    '''
    
    name = 'Common Log Format'
    format = None

    def __init__(self, sink):
        super(CommonLogFormatParser, self).__init__(sink)
        self.apachelog_parser = apachelog.parser(self.format)

    def parse(self, line):
        try:
            info = self.apachelog_parser.parse(line)

            uri = None
            # При истечении времени обработки запроса (408) nginx не пишет путь???
            if info['%r'] != '-':
                try:
                    method, uri, http_version = info['%r'].split(' ')
                except ValueError:
                    pass
                else:
                    # Убираем GET-параметры из запроса
                    delim_pos = uri.find('?')
                    if delim_pos > 0:
                        uri = uri[:delim_pos]

                    # Преобразуем URI в удобоваримую форму
                    uri = re.sub('[\.|/|-]', '_', uri.lstrip('/'))

            time_str = info['%t'][1:-1]

            if time_str[-5] == '+':
                time_str = time_str[:-6]

            request_date = datetime.datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')
            timestamp = time.mktime(request_date.timetuple())

            # Некоторые веб-серверы (gunicorn до определенных версий)
            # не пишут размер ответа в лог, поэтому нужна явная проверка.
            if uri:
                if info['%b'] != 'None' and info['%>s'] == '200':
                    self.sink.emit('timing', 'response.%s.size' % uri, info['%b'], timestamp=timestamp)
                self.sink.emit('timing', 'response.%s.%s' % (uri, info['%>s']), '1', timestamp=timestamp)

            self.sink.emit('counter', 'response.code.%s' % info['%>s'], '1',
                           timestamp=timestamp)
        except apachelog.ApacheLogParserError:
            # TODO: логирование поломанных строчек
            logging.debug(u"Line can't be parsed: %s" % line)


class GunicornParser(CommonLogFormatParser):
    u'''
    Обработка стандартного журнала, выдаваемого директивой access_log gunicorn'а.
    До определенной версии gunicorn не записывал размер возвращаемого ответа :-(
    '''
    name = 'gunicorn access_log parser'
    format = r'%h - - %t \"%r\" %>s %b \"%u\" \"%{User-Agent}i\"'


class NginxParser(CommonLogFormatParser):
    u'''
    Обработка стандартного журнала, выдаваемого директивой access_log.
    '''    
    name = 'nginx access_log parser'
    format = r'%V %h %u %t \"%r\" %>s %b \"%i\" \"%{User-Agent}i\"'
