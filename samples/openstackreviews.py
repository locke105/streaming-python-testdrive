from __future__ import print_function

import json
from pprint import pprint
import random
import sys
import threading
import time

import six
from streamsx.topology import topology
from streamsx.topology import context
from streamsx.topology import functions
from streamsx import rest

from speyer import client
from speyer.ingest import gerrit


INGEST_JOB_NAME = 'os_review_ingester'
FILTER_JOB_NAME = 'os_review_filter'
COMMENT_VIEW_NAME = 'comment_view'

config_json = None


def build_streams_config(service_name, credentials):
    vcap_conf = {
        'streaming-analytics': [
            {
                'name': service_name,
                'credentials': credentials,
            }
        ]
    }
    config = {
        context.ConfigParams.VCAP_SERVICES: vcap_conf,
        context.ConfigParams.SERVICE_NAME: service_name,
        context.ConfigParams.FORCE_REMOTE_BUILD: True
    }
    return config


class ReservoirSample(object):

    def __init__(self, size=30):
        self.count = 0
        self.size = size
        self.samples = []

    def update(self, data):
        if self.count < self.size:
            self.samples.append(data)
        else:
            chance = random.randint(0, self.count)
            if chance < self.size:
                self.samples[chance] = data
        self.count += 1


def get_config(filename='openstackreviews.json'):
    global config_json
    if config_json is None:
        with open(filename) as config_file:
            config_json = json.load(config_file)
    return config_json


def build_ingest_app(config, streams_config):
    gerrit_stream = gerrit.GerritEvents(**config['gerrit'])

    ingest_topo = topology.Topology(INGEST_JOB_NAME)
    event_stream = ingest_topo.source(gerrit_stream)
    event_stream.publish('openstack_review_stream')

    # ship it off to service to build and run
    context.submit('ANALYTICS_SERVICE', ingest_topo, config=streams_config)


def build_filter_app(config, streams_config):

    def comments_only(data):
        jdata = json.loads(data)
        print(jdata, file=sys.stderr)
        return jdata["type"] == "comment-added"

    # define our streams app
    filter_topo = topology.Topology(FILTER_JOB_NAME)
    event_stream2 = filter_topo.subscribe('openstack_review_stream')
    comments_stream = event_stream2.filter(comments_only)
    comments_stream.publish('comments')
    c_view = comments_stream.view(name=COMMENT_VIEW_NAME)

    # ship it off to service to build and run
    context.submit('ANALYTICS_SERVICE', filter_topo, config=streams_config)


def summarize(sample):
    authors_count = {}
    for item in sample.samples:
        evt = json.loads(item)
        val = evt['author']['name']
        if val in authors_count:
            authors_count[val] += 1
        else:
            authors_count[val] = 1
    return authors_count


def main():
    redeploy = True

    config = get_config()
    streams_config = build_streams_config(**config['streams'])

    sas = client.StreamsAnalyticsService(config['streams']['credentials'])

    def find_job_by_name(jobs, name):
        for job in jobs:
            if name in job['name']:
                return job

    if redeploy:
        # clean up old jobs, since we are going to redeploy
        job_names = [FILTER_JOB_NAME, INGEST_JOB_NAME]
        for name in job_names:
            job = find_job_by_name(sas.jobs()['jobs'], name)
            if job:
                sas.cancel_job(job['jobId'])

        build_ingest_app(config, streams_config)
        build_filter_app(config, streams_config)

    current_jobs = sas.jobs()['jobs']
    assert(find_job_by_name(current_jobs, FILTER_JOB_NAME)['health'] == 'healthy')
    assert(find_job_by_name(current_jobs, INGEST_JOB_NAME)['health'] == 'healthy')

    c_view = None
    sws = rest.StreamsContext(**sas.get_sws_info())
    for view in sws.get_instances()[0].get_views():
        print(view.name)
        if view.name == COMMENT_VIEW_NAME:
            c_view = view

    if c_view is None:
        raise Exception(
            'Could not find view for name: {}'.format(COMMENT_VIEW_NAME))

    rsample = ReservoirSample(size=200)
    stopping = False
    def view_handler(view):
        try:
            queue = view.start_data_fetch()
            while not stopping:
                rsample.update(queue.get())
        finally:
            view.stop_data_fetch()


    sample_thread = threading.Thread(
        target=view_handler,
        args=(c_view,))

    sample_thread.start()

    try:
        while True:
            time.sleep(10)
            pprint(summarize(rsample))
    finally:
        stopping = True
        sample_thread.join()


if __name__ == '__main__':
    main()
