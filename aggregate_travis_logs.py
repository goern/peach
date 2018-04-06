#!/usr/bin/env python
#
#   peach
#   Copyright(C) 2018 Christoph Görn
#
#   This program is free software: you can redistribute it and / or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Thoth: TravisBuildLogAggregator"""

import os
import json
import codecs
import logging

import luigi
from tornado import httpclient

from luigi_s3_target import S3Target

__version__ = '0.2.0-dev'


# get our configuration from ENV
DEBUG = bool(os.getenv('DEBUG', False))
FILES_ON_CEPH = bool(os.getenv('PEACH_FILES_ON_CEPH', False))
BUCKET_NAME = os.getenv('THOTH_CEPH_BUCKET_NAME')
BUCKET_PREFIX = os.getenv('THOTH_CEPH_BUCKET_PREFIX')
TRAVIS_TOKEN = os.getenv('PEACH_TRAVIS_TOKEN')

# HTTP request headers used to talk to Travis CI API
HEADERS = {'Travis-API-Version': '3',
           'User-Agent': 'ThothTravisBuildLogAggregator/'+__version__+' luigi/2.7.3',
           'Authorization': 'token ' + TRAVIS_TOKEN}

if DEBUG:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class AggregateRepositories(luigi.Task):
    """Aggregate all Repositories of one Owner/Organisation/User and store the list of repositories in a TSV file

    :param owner: Owner/Organisation/User
    """

    retry_count = 2

    owner = luigi.Parameter()

    def output(self):
        if FILES_ON_CEPH:
            return S3Target(f's3://{BUCKET_NAME}/{BUCKET_PREFIX}/{self.owner}/repositories.tsv')
        else:
            return luigi.LocalTarget(f'data/{self.owner}/repositories.tsv')

    def run(self):
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader("utf-8")

        try:
            response = http_client.fetch(
                'https://api.travis-ci.org/owner/{}/repos'.format(self.owner), headers=HEADERS)

            _response = json.load(reader(response.buffer))

            with self.output().open('wb') as outfile:
                for entry in _response['repositories']:
                    logger.debug(
                        f"writing Repository id={entry['id']} to output")

                    if FILES_ON_CEPH:
                        outfile.write(f"{entry['id']}\n".encode())
                    else:
                        print(f"{entry['id']}", file=outfile)

        except httpclient.HTTPError as e:
            logger.error("Error: " + str(e))
        except Exception as e:
            logger.error("Error: " + str(e))

        http_client.close()


class GetAllTravisBuilds(luigi.Task):
    """GetAllTravisBuilds will download all the Build information from Travis and store them to one JSON file."""
    owner = luigi.Parameter()

    def getPageFromTravis(self, url, offset, limit=100):
        """Paginate thru travis API"""
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader('utf-8')

        try:
            response = http_client.fetch(
                '{}?offset={}&limit={}'.format(url, offset, limit), headers=HEADERS)

            return json.load(reader(response.buffer))

        except httpclient.HTTPError as e:
            logger.error("Error: " + str(e))
        except Exception as e:
            logger.error("Error: " + str(e))

        http_client.close()

        return None

    def output(self):
        if FILES_ON_CEPH:
            return S3Target('s3://{}/{}/{}/builds.json'.format('DH-DEV-DATA', 'data/thoth-travis', self.owner))
        else:
            return luigi.LocalTarget('data/{}/builds.json'.format(self.owner))

    def run(self):
        is_last = False
        offset = -100
        limit = 100

        all_builds = []

        while not is_last:
            builds = self.getPageFromTravis(
                'https://api.travis-ci.org/builds', offset + limit)
            logger.debug(json.dumps(builds['@pagination']))

            is_last = builds['@pagination']['is_last']
            offset = builds['@pagination']['offset']
            limit = builds['@pagination']['limit']

            all_builds.extend(builds['builds'])

        with self.output().open('wb') as outfile:
            json.dump(all_builds, outfile)


class AggregateJobs(luigi.Task):
    retry_count = 4

    owner = luigi.Parameter()

    def output(self):
        if FILES_ON_CEPH:
            return S3Target(f's3://{BUCKET_NAME}/{BUCKET_PREFIX}/{self.owner}/jobs.tsv')
        else:
            return luigi.LocalTarget(f'data/{self.owner}/jobs.tsv')

    def requires(self):
        return AggregateRepositories(self.owner)

    def run(self):
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader('utf-8')

        # lets read all the repo id
        infile = self.input().open('rb')
        repos = infile.read().splitlines()

        try:
            with self.output().open('wb') as outfile:
                for repo in repos:
                    if FILES_ON_CEPH:
                        repo = repo.decode('utf-8')

                    logger.debug('working on repo {}'.format(repo))

                    response = http_client.fetch(
                        f'https://api.travis-ci.org/repo/{repo}/builds', headers=HEADERS)

                    logger.debug(
                        'got response from Travis API: {}'.format(response))

                    _response = json.load(reader(response.buffer))

                    for entry in _response['builds']:
                        for job in entry['jobs']:
                            logger.debug(
                                'writing Job id={} to output'.format(job['id']))

                            if FILES_ON_CEPH:
                                outfile.write(f"{job['id']}\n".encode())
                            else:
                                print(f"{job['id']}", file=outfile)

        except httpclient.HTTPError as e:
            logger.error(f"Error: {e}: repo id: {repo}")
        except Exception as e:
            logger.error("Error: " + str(e))

        http_client.close()


class AggregateTravisLogs(luigi.Task):
    owner = luigi.Parameter()

    def requires(self):
        return AggregateJobs(self.owner)

    def run(self):
        infile = self.input().open('rb')
        jobs = infile.read().splitlines()

        fetch_logs = []

        for job in jobs:
            if FILES_ON_CEPH:
                job = job.decode('utf-8')

            fetch_logs.append(FetchLog(job, self.owner))

        yield fetch_logs


class FetchLog(luigi.Task):
    job = luigi.Parameter()
    owner = luigi.Parameter()

    def output(self):
        if FILES_ON_CEPH:
            return S3Target(f's3://{BUCKET_NAME}/{BUCKET_PREFIX}/{self.owner}/jobs/{self.job}/log.json')
        else:
            return luigi.LocalTarget(f'data/{self.owner}/jobs/{self.job}/log.json')

    def run(self):
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader('utf-8')

        try:
            response = http_client.fetch(
                f'https://api.travis-ci.org/job/{self.job}/log', headers=HEADERS)

            _response = json.load(reader(response.buffer))

            with self.output().open('wb') as outfile:
                if FILES_ON_CEPH:
                    outfile.write(response.body)
                else:
                    json.dump(_response, outfile)

        except httpclient.HTTPError as e:
            logger.error("Error: " + str(e))
        except Exception as e:
            logger.error("Error: " + str(e))

        http_client.close()

        yield FetchRawLog(self.job, self.owner)


class FetchRawLog(luigi.Task):
    job = luigi.Parameter()
    owner = luigi.Parameter()

    def output(self):
        if FILES_ON_CEPH:
            return S3Target(f's3://{BUCKET_NAME}/{BUCKET_PREFIX}/{self.owner}/jobs/{self.job}/log.txt')
        else:
            return luigi.LocalTarget(f'data/{self.owner}/jobs/{self.job}/log.txt')

    def run(self):
        http_client = httpclient.HTTPClient()

        try:
            response = http_client.fetch(
                f'https://api.travis-ci.org/v3/job/{self.job}/log.txt', headers=HEADERS)

            with self.output().open('wb') as outfile:
                if FILES_ON_CEPH:
                    outfile.write(response.body)
                else:
                    print(response.body.decode('utf8'), file=outfile)

        except httpclient.HTTPError as e:
            logger.error("Error: " + str(e))
        except Exception as e:
            logger.error("Error: " + str(e))

        http_client.close()


if __name__ == '__main__':
    try:
        os.mkdir('data')
    except FileExistsError as e:
        pass

    luigi.build([AggregateTravisLogs('radanalyticsio'),
                 GetAllTravisBuilds('goern')])
