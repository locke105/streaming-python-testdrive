from __future__ import print_function

import select

import paramiko


class GerritEvents(object):

    def __init__(self, userid, host, key=None):
        self.userid = userid
        self.host = host
        self.port = 29418
        self.key = key


    def _read_events(self, stream, use_poll=False):
        if not use_poll:
            yield stream.readline().strip()

        poller = select.poll()
        poller.register(stream.channel)
        while True:
            for fd, event in poller.poll():
                if fd == stream.channel.fileno():
                    if event == select.POLLIN:
                        yield stream.readline().strip()
                    else:
                        raise Exception('Non-POLLIN event on stdout!')

    @property
    def events(self):
        client = paramiko.SSHClient()
        client.load_system_host_keys()

        connargs = {
            'hostname': self.host,
            'port': self.port,
            'username': self.userid
        }
        if self.key:
            connargs['pkey'] = key

        client.connect(**connargs)

        stdin, stdout, stderr = client.exec_command('gerrit stream-events')

        for event in self._read_events(stdout, use_poll=True):
            yield event
