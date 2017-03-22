import requests

import requests.packages.urllib3


requests.packages.urllib3.disable_warnings()


class StreamsAnalyticsService(requests.Session):

    def __init__(self, credentials):
        super(StreamsAnalyticsService, self).__init__()
        self.auth = (credentials['userid'], credentials['password'])
        self.creds = credentials

    def jobs(self):
        resp = self.get(
           self.creds['rest_url'] + self.creds['jobs_path'])
        resp.raise_for_status()
        return resp.json()

    def start_instance(self):
        url = self.creds['rest_url'] + self.creds['start_path']
        resp = self.put(
            url=url,
            headers={'accept': 'application/json',
                     'content-type': 'application/json'})
        resp.raise_for_status()
        return resp

    def _job_action(self, action, job_id=None, job_name=None):
        if job_id is None and job_name is None:
            raise ValueError('Must specify job_id or job_name!')

        if job_id is not None:
            params = {'job_id': job_id}
        else:
            params = {'job_name': job_name}

        resp = self.request(
            method=action,
            url=(self.creds['rest_url'] + self.creds['jobs_path']),
            params=params)
        resp.raise_for_status()
        return resp.json()

    def get_job(self, job_id=None, job_name=None):
        return self._job_action(
            action='GET', job_id=job_id, job_name=job_name)

    def cancel_job(self, job_id=None, job_name=None):
        return self._job_action(
            action='DELETE', job_id=job_id, job_name=job_name)

    def get_sws_info(self):
        resp = self.get('' + self.creds['rest_url'] + self.creds['resources_path'])
        sas_data = resp.json()
        sws_url = sas_data['streams_rest_url'] + '/resources'
        return {'username': self.creds['userid'],
                'password': self.creds['password'],
                'resource_url': sws_url}
