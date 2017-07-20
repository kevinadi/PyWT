#!/usr/bin/env python
'''
Based on https://github.com/wiredtiger/wiredtiger/blob/master/examples/python/ex_access.py
'''

import argparse
from pprint import pformat
import bson
from bson import json_util
from wiredtiger import wiredtiger_open

class PyWT(object):
    ''' Python WiredTiger '''

    def __init__(self, dbpath):
        ''' Connect to the database and open a session '''
        conn = wiredtiger_open(dbpath, 'create')
        self.session = conn.open_session()
        self.dpath = dbpath

    @staticmethod
    def bson_decode(content):
        ''' Returns decoded BSON obect '''
        return bson.BSON.decode(bson.BSON(content))

    def find_table_name(self, namespace):
        ''' Find the corresponding WT table name from MongoDB namespace '''
        cursor = self.session.open_cursor('table:_mdb_catalog', None)
        for _, value in cursor:
            val = PyWT.bson_decode(value)
            if val.get('ns') == namespace:
                return self.dump_table(str(val.get('ident')), False, False)
        return ''

    def insert_table(self, table):
        ''' Insert 5 records into the table '''
        self.session.create('table:'+table, 'key_format=S,value_format=S')
        self.session.begin_transaction()
        cursor = self.session.open_cursor('table:'+table, None)
        for idx in range(5):
            cursor.set_key('key' + str(idx))
            cursor.set_value('value' + str(idx))
            cursor.insert()
        self.session.rollback_transaction()
        return True

    def dump_table(self, table, raw, pretty):
        ''' Dump the table contents (assumes BSON-encoded) '''
        cursor = self.session.open_cursor('table:'+table, None)
        output = ''
        for key, value in cursor:
            if not raw:
                val = PyWT.bson_decode(value)
            else:
                val = value
            if pretty:
                output += '-----Key: {key}-----\n{val}\n\n'.format(key=key, val=pformat(val))
            else:
                output += '{val}\n'.format(val=json_util.dumps(val))
        cursor.close()
        return output

    def dump_catalog(self):
        ''' Dump the _mdb_catalog table '''
        cursor = self.session.open_cursor('table:_mdb_catalog', None)
        sizes = self.session.open_cursor('table:sizeStorer', None)
        output = ''
        for _, value in cursor:
            val = PyWT.bson_decode(value)
            table = val.get('ident')
            namespace = val.get('ns')
            indexes = val.get('idxIdent')

            if not namespace:
                continue
            print 'MongoDB namespace : {ns}'.format(ns=namespace)
            print 'WiredTiger table  : {tbl}'.format(tbl=table)

            sizes.set_key('table:'+str(table))
            if sizes.search() == 0:
                wtsizes = PyWT.bson_decode(sizes.get_value())
                datasize = wtsizes.get('dataSize')
                numrecords = wtsizes.get('numRecords')
                print 'Data Size         : {0} bytes ({1} MB)'.format(datasize, datasize / 1024**2)
                print 'Num Records       : {0}'.format(numrecords)

            if indexes:
                print 'Indexes :'
                for index in sorted(indexes):
                    print '    {1} : {0}'.format(index, indexes.get(index))

            print
        cursor.close()
        sizes.close()
        return output


if __name__ == '__main__':
    # pylint: disable=invalid-name
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbpath', default='/data/db', help='dbpath')
    parser.add_argument('--list', action='store_true', help='print MongoDB catalog content')
    parser.add_argument('--raw', action='store_true', help='print raw data')
    parser.add_argument('--pretty', action='store_true', help='pretty print documents')
    parser.add_argument('--table', help='WT table to print')
    parser.add_argument('--export', help='MongoDB namespace to export')
    args = parser.parse_args()

    wt = PyWT(args.dbpath)
    if args.list:
        print wt.dump_catalog()
    elif args.table:
        print wt.dump_table(args.table, args.raw, args.pretty)
    elif args.export:
        print wt.find_table_name(args.export)
