#!/usr/bin/env python

# Run with py.test xxxx.py
# OR
# py.test --junitxml results.xml xxxx.py
# OR make directly executable

# if __name__ == '__main__':
#     unittest.main()

# suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
# unittest.TextTestRunner(verbosity=2).run(suite)


# --------- Testing Preparation -----------
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Date, DateTime, Numeric
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker

import datetime
from datetime import date

import decimal

try:
    import unittest2 as unittest
except ImportError:
    print "Please consider to install unittest2"
    import unittest

import pytest


@pytest.mark.usefixtures("params")
class MulticornBaseTest(unittest.TestCase):
    '''
    Create the test framework for running tests on posgresql
    '''

    @classmethod
    def setUpClass(cls):
        print 'Creating new engine for DB with username'
        cls.engine = None
        cls.Session = None
        # cls.engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (username, password, db), echo=True)

    @classmethod
    def tearDownClass(cls):
        print 'Releasing DB (TODO)'

    def setUp(self):
        if not self.engine:
            print 'Creating new DB Engine'
            self.engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (self.username, self.password, self.db), echo=True)
            self.metadata = MetaData()

        if not self.Session:
            print "Creating new Session Factory"
            self.Session = sessionmaker(bind=self.engine)

        self.session = self.Session()

        self.session.begin(subtransactions=True)
        # self.conn = self.engine.connect()
        # Need to force a transaction and subsequent commit as sqlalchemy 'cleverly detects' commit type lanugage and auto triggers commits. But fails to see executing functions using SELECT as requiring a commit()
        # self.trans = self.conn.begin()

    def tearDown(self):
        print 'About to commit DB session'
        self.session.commit()
        self.session.close()
        print 'Released DB session'

    def query_compare(self, ref_table, test_table, query_string, matching_type):
        cursor = self.conn.execute(query_string.format(table_name))

    def query_execute(self, table_name, query_string, matching_pks):
        cursor = self.conn.execute(query_string.format(table_name))
        if type(matching_pks) is int:
            returnVal = cursor.scalar()
            self.assertEqual(returnVal, matching_pks, msg="Expecting %s, found: %s" % (matching_pks, returnVal))
        else:
            foundRecords = []

            for row in cursor:
                with self.subTest(row=row):
                    testRow = findRow(row[pkColumn])
                    foundRecords.append(row[pkColumn])

                    for (columnReturned, columnDescription) in zip(row, cursor.keys()):
                        self.assertEqual(testRow[columnStructure[columnDescription]], columnReturned, msg="Mismatch in '%s':%s Expecting: %s Found: %s" % (row, columnDescription, testRow[columnStructure[columnDescription]], columnReturned))

            if type(matching_pks) is set:
                self.assertFalse(set(matching_pks) ^ set(foundRecords), msg="Did not find the right matching records: found %s, with query: %s" % (foundRecords, query_string))
            else:
                self.assertFalse(cmp(matching_pks, foundRecords), msg="Did not find the right sequence of records: found %s, with query: %s" % (foundRecords, query_string))

    def test_connection_check(self):
        sqlReturn = self.session.execute('SELECT')

        self.assertTrue(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
        self.assertEqual(len(sqlReturn.fetchall()), 1)
