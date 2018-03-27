# Thoth Log Aggregator

This will fetch logs from travis...

## Usage

```shell
pipenv install
luigid --logdir=. --state-path=./luigi-state.pkl --background
time PYTHONPATH='.' luigi --module aggregate_travis_logs AggregateLogs --owner=goern
```