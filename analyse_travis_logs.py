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

"""Thoth: TravisLogAnalyser"""

import os
import json
import tempfile
import re
import csv

import luigi
from luigi.contrib.spark import PySparkTask

__version__ = '0.1.0'


class ProcessTravisJob(PySparkTask):
    owner = luigi.Parameter()
    job_id = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/jobs/{}/summary.json'.format(self.owner, self.job_id))

    def main(self, sc, * args):
        pass

    def run(self):
        result = {}  # This is our analyser result
        result['succeeded'] = True
        result['owner_slug'] = self.owner
        result['job_id'] = self.job_id

        ANSI_ESCAPE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')

        with open(
                'data/{}/jobs/{}/log.txt'.format(self.owner, self.job_id), 'r') as infile, self.output().open('w') as outfile:

            for line in infile:
                # strip all Ascii ESC seqs
                line = ANSI_ESCAPE.sub('', str(line))

                if line.startswith('Done. Your build exited with'):
                    if '0.' not in line.split(' ')[-1]:
                        result['succeeded'] = False

            json.dump(result, outfile)


class ProcessTravisJobs(luigi.Task):
    owner = luigi.Parameter()

    def input(self):
        return luigi.LocalTarget("data/{}/jobs.tsv".format(self.owner))

    def requires(self):
        jobs = []

        with self.input().open('r') as infile:
            rd = csv.reader(infile, delimiter="\t", quotechar='"')
            for row in rd:
                jobs.append(ProcessTravisJob(owner=self.owner, job_id=row[0]))

        return jobs


if __name__ == '__main__':
    try:
        os.mkdir('data')
    except FileExistsError as e:
        pass

    luigi.build(
        [ProcessTravisJobs('goern')])
