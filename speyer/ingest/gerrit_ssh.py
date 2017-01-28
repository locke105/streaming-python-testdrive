from __future__ import print_function

import subprocess

try:
    from subprocess import DEVNULL
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'r')


class GerritEvents(object):

    def __init__(self, userid, host):
        self.userid = userid
        self.host = host
        self.port = 29418

    @property
    def events(self):
        cmd = ('ssh -p {self.port} {self.userid}@{self.host} '
               'gerrit stream-events')
        cmd = cmd.format(self=self)

        proc = subprocess.Popen(
            cmd.split(' '),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=DEVNULL)

        try:
            for item in iter(proc.stdout.readline, ''):
                yield item.strip()
        finally:
            proc.terminate()
