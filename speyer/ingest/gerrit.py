from __future__ import print_function

import logging
import select

import paramiko
from paramiko import rsakey
import six


LOG = logging.getLogger(__name__)


class GerritEvents(object):
    """
    Build and use a GerritEvents object to iterate
    over all incoming events from a Gerrit server.

      import json
      import pprint

      gerrit_stream = gerrit.GerritEvents(
          userid='your_gerrit_user_id',
          host='review.example.org')

      for event in gerrit_stream.events():
          pprint.pprint(json.loads(event))

    Optionally, you can pass a private key in directly as
    a string. If 'key' is not specified, the default is to rely
    on Paramiko to find a default, usually this is '~/.ssh/id_rsa'.

    The folllowing is an example of passing in the private key directly::

      key = '''
      -----BEGIN RSA PRIVATE KEY-----
      .............................
      -----END RSA PRIVATE KEY-----
      '''

      gerrit_stream = gerrit.GerritEvents(
          userid='your_gerrit_user_id',
          host='review.example.org',
          key=key)

    """

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

    def events(self):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())

        connargs = {
            'hostname': self.host,
            'port': self.port,
            'username': self.userid
        }
        if self.key:
            keyfile = six.moves.StringIO(self.key)
            pkey = rsakey.RSAKey(file_obj=keyfile)
            connargs['pkey'] = pkey

        client.connect(**connargs)
        LOG.info('Connected to gerrit')

        stdin, stdout, stderr = client.exec_command('gerrit stream-events')

        for event in self._read_events(stdout, use_poll=True):
            yield event

    def __call__(self):
        for event in self.events():
            yield event
