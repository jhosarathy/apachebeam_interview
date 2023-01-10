# Data Analysis

## Development Environment

```
Python 3.8.10
Pip 21.3.1
Virtualenv 20.3.1
```

## Setup

```
# Install pip3
$ sudo apt-get install python3-pip
# Install virtualenv
$ sudo pip3 install virtualenv

$ cd 2023_virginmedia_interview
$ virtualenv venv
$ source ./venv/bin/activate

# Install python dependencies of the current app
$ pip install -r requirements.txt
```

## Development

```
# Task1 Python Command
$ python3 src/pipeline.py --input-file gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv -o ./output/results.json

# Task2 Python Command
$ python3 src/pipeline2.py --input-file gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv -o ./output/results.json
```

To read the output

```
$ cd output
$ gunzip results.json-00000-of-00001.gz
```

## Testing

```
$ cd virginmedia_interview
$ source ./venv/bin/activate
$ PYTHONPATH=./ python3 -m unittest test/unit/test_pipeline2.py
```
