# PyWT
Python script to delve into MongoDB's WiredTiger data files

## Requirements

* `pip install pymongo`
* `pip install wiredtiger`

Please note that proper tooling is required to install the WiredTiger Python module. Please refer to [the WiredTiger documentation](http://source.wiredtiger.com/develop/build-posix.html) for instructions on how to successfully compile WiredTiger.

## Usage

```
usage: PyWT.py [-h] [--dbpath DBPATH] [--list] [--raw] [--pretty]
               [--table TABLE] [--export EXPORT]

optional arguments:
  -h, --help       show this help message and exit
  --dbpath DBPATH  dbpath
  --list           print MongoDB catalog content
  --raw            print raw data
  --pretty         pretty print documents
  --table TABLE    WT table to print
  --export EXPORT  MongoDB namespace to export
```

## Examples

List MongoDB namespace <-> WiredTiger tables:

```
PyWT.py --dbpath /data/db --list
```

Pretty print the contents of a specific WiredTiger table:

```
PyWT.py --table _mdb_catalog --pretty
```

Restore a specific MongoDB namespace to a running MongoDB instance:

```
PyWT.py --export test.collection | mongoimport -d test -c collection
```
