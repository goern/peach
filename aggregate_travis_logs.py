#!/usr/bin/env python
#
#   thoth-dependency-monkey
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

"""Thoth: LogAggregator"""

import os
import json
import codecs
import tempfile

import luigi
from tornado import httpclient


__version__ = '0.1.0'

TRAVIS_TOKEN = 'FlNtKcTAawHcpFTvxprCqA'
HEADERS = {'Travis-API-Version': '3',
           'User-Agent': 'ThothLogAggragator/0.1.0 luigi/2.5.7',
           'Authorization': 'token ' + TRAVIS_TOKEN}


class AggregateRepositories(luigi.Task):
    owner = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("data/{}/repositories.tsv".format(self.owner))

    def run(self):
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader("utf-8")

        try:
            response = http_client.fetch(
                'https://api.travis-ci.org/owner/{}/repos'.format(self.owner), headers=HEADERS)

            _response = json.load(reader(response.buffer))

            try:
                os.mkdir('data/{}'.format(self.owner))
            except FileExistsError as e:
                pass

            with open('data/{}/repositories.json'.format(self.owner), 'w') as jsonoutfile:
                json.dump(_response, jsonoutfile)

            with self.output().open('w') as outfile:
                for entry in _response['repositories']:
                    print(entry['id'], file=outfile)

        except httpclient.HTTPError as e:
            print("Error: " + str(e))
        except Exception as e:
            print("Error: " + str(e))

        http_client.close()


def getPageFromTravis(url, offset, limit=100):
    http_client = httpclient.HTTPClient()
    reader = codecs.getreader('utf-8')

    try:
        response = http_client.fetch(
            '{}?offset={}&limit={}'.format(url, offset, limit), headers=HEADERS)

        return json.load(reader(response.buffer))
    except httpclient.HTTPError as e:
        print("Error: " + str(e))

    except Exception as e:
        print("Error: " + str(e))

    http_client.close()

    return None


class GetAllBuilds(luigi.Task):
    owner = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/builds.json'.format(self.owner))

    def run(self):
        is_last = False
        offset = -100
        limit = 100

        all_builds = []

        while not is_last:
            builds = getPageFromTravis(
                'https://api.travis-ci.org/builds', offset + limit)
            print(json.dumps(builds['@pagination']))

            is_last = builds['@pagination']['is_last']
            offset = builds['@pagination']['offset']
            limit = builds['@pagination']['limit']

            all_builds.extend(builds['builds'])

        with self.output().open('w') as outfile:
            json.dump(all_builds, outfile)


class AggregateJobs(luigi.Task):
    owner = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/jobs.tsv'.format(self.owner))

    def requires(self):
        return AggregateRepositories(self.owner)

    def run(self):
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader('utf-8')

        # maybe we need that directory later...
        try:
            os.mkdir('data/{}'.format(self.owner))
        except FileExistsError as e:
            pass

        # lets read all the repo id
        with self.input().open('r') as infile:
            repos = infile.read().splitlines()

        try:
            with self.output().open('w') as outfile:
                for repo in repos:
                    response = http_client.fetch(
                        'https://api.travis-ci.org/repo/{}/builds'.format(repo), headers=HEADERS)

                    _response = json.load(reader(response.buffer))

                    for entry in _response['builds']:
                        for job in entry['jobs']:
                            print(job['id'], file=outfile)

        except httpclient.HTTPError as e:
            print("Error: " + str(e))
        except Exception as e:
            print("Error: " + str(e))

        http_client.close()


class AggregateLogs(luigi.Task):
    owner = luigi.Parameter()

    def requires(self):
        return AggregateJobs(self.owner)

    def run(self):
        with self.input().open('r') as infile:
            jobs = infile.read().splitlines()

        fetch_logs = [FetchLog(j, self.owner) for j in jobs]
        yield fetch_logs


class FetchLog(luigi.Task):
    job = luigi.Parameter()
    owner = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/jobs/{}/log.json'.format(self.owner, self.job))

    def run(self):
        http_client = httpclient.HTTPClient()
        reader = codecs.getreader('utf-8')

        try:
            response = http_client.fetch(
                'https://api.travis-ci.org/job/{}/log'.format(self.job), headers=HEADERS)

            _response = json.load(reader(response.buffer))

            with self.output().open('w') as outfile:
                json.dump(_response, outfile)

        except httpclient.HTTPError as e:
            print("Error: " + str(e))
        except Exception as e:
            print("Error: " + str(e))

        http_client.close()

        yield FetchRawLog(self.job, self.owner)


class FetchRawLog(luigi.Task):
    job = luigi.Parameter()
    owner = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/jobs/{}/log.txt'.format(self.owner, self.job))

    def run(self):
        http_client = httpclient.HTTPClient()

        try:
            response = http_client.fetch(
                'https://api.travis-ci.org/v3/job/{}/log.txt'.format(self.job), headers=HEADERS)

            with self.output().open('wb') as outfile:
                print(response.body.decode('utf8'), file=outfile)

        except httpclient.HTTPError as e:
            print("Error: " + str(e))
        except Exception as e:
            print("Error: " + str(e))

        http_client.close()


if __name__ == '__main__':
    try:
        os.mkdir('data')
    except FileExistsError as e:
        pass

    luigi.build([AggregateLogs('radanalyticsio'),
                 AggregateLogs('manageiq'),
                 GetAllBuilds('goern')])
