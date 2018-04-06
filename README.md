# Thoth Log Aggregator and Analyser

This will fetch logs from travis...

## Usage

```shell
pipenv install
luigid --logdir=. --state-path=./luigi-state.pkl --background
time PYTHONPATH='.' luigi --module aggregate_travis_logs AggregateTravisLogs --owner=goern
time PYTHONPATH='.' luigi --module analyse_travis_logs ProcessTravisJobs --owner=goern

```