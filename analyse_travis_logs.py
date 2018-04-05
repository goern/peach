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


__version__ = '0.1.0'


class ProcessTravisJob(luigi.Task):
    owner = luigi.Parameter()
    job_id = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/jobs/{}/summary.txt'.format(self.owner, self.job_id))

    def run(self):
        ANSI_ESCAPE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')

        with open(
                'data/{}/jobs/{}/log.txt'.format(self.owner, self.job_id), 'r') as infile, self.output().open('w') as outfile:

            for line in infile:
                outfile.write(ANSI_ESCAPE.sub('', str(line)))


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
