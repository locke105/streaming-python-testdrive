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

    def _job_action(self, action, job_id=None, job_name=None):
        if job_id is None and job_name is None:
            raise ValueError('Must specify job_id or job_name!')

        if job_id:
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
