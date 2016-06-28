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
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import text

import datetime
from datetime import date

import decimal

try:
    import unittest2 as unittest
except ImportError:
    print "Please consider to install unittest2"
    import unittest

import pytest
# Need to ensure: pip install pytest-ordering


# class TestStringMethods(unittest.TestCase):
#
#     def test_upper(self):
#         self.assertEqual('foo'.upper(), 'FOO')
#
#     def test_isupper(self):
#         self.assertTrue('FOO'.isupper())
#         self.assertFalse('Foo'.isupper())
#
#     def test_split(self):
#         s = 'hello world'
#         self.assertEqual(s.split(), ['hello', 'world'])
#         # check that s.split fails when the separator is not a string
#         with self.assertRaises(TypeError):
#             s.split(2)

testDataSrc = [
    (1, '1980-01-01', '1980-01-01  11:01:21.132912', 3.4, 'Test'),
    (2, '1990-03-05', '1998-03-02  10:40:18.321023', 12.2, 'Another Test'),
    (3, '1972-01-02', '1972-01-02  16:12:54', 4000, 'another Test'),
    (4, '1922-11-02', '1962-01-02  23:12:54', -3000, None)]


def from_sqlish(input):
    return (
        input[0],
        datetime.datetime.strptime(input[1], '%Y-%m-%d').date(),
        datetime.datetime.strptime(input[2], '%Y-%m-%d  %H:%M:%S.%f') if '.' in input[2] else datetime.datetime.strptime(input[2], '%Y-%m-%d  %H:%M:%S'),
        input[3],
        input[4])


def to_sql(input):
    return "(%s, %s, %s, %s, %s)" % (
        input[0] if input[0] is not None else 'NULL',
        "'"+input[1].isoformat()+"'" if input[1] is not None else 'NULL',
        "'"+input[2].isoformat(' ')+"'" if input[2] is not None else 'NULL',
        decimal.Decimal(input[3]) if input[3] is not None else 'NULL',
        "'"+input[4]+"'" if input[4] is not None else 'NULL')

testData = [from_sqlish(row) for row in testDataSrc]
pkColumn = 0
columnStructure = {'id': 0, 'adate': 1, 'atimestamp': 2, 'anumeric': 3, 'avarchar': 4}

query_tests = [
    ('''SELECT * FROM basetable''', [1, 2, 3, 4]),
    ('''SELECT id,atimestamp FROM basetable''', [1, 2, 3, 4]),
    ('''SELECT * FROM basetable WHERE avarchar IS NULL''', [4]),
    ('''SELECT * FROM basetable WHERE avarchar IS NOT NULL''', [1, 2, 3])
]


def findRow(pk):
    return [row for row in testData if row[0] == pk][0]


class TestConnectionToPostgres(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'setUpClass'
        cls.engine = create_engine('postgresql://demo:demo@localhost:5432/demo', echo=True)

    @classmethod
    def tearDownClass(cls):
        print 'tearDownClass'
        # self.close()

    def setUp(self):
        print "setUp"
        self.conn = self.engine.connect()

    def tearDown(self):
        print 'tearDown'
        self.conn.close()

    def query_execute(self, query_string, matching_pks):
        cursor = self.conn.execute(query_string)
        foundRecords = []

        for row in cursor:
            with self.subTest(row=row):
                testRow = findRow(row[pkColumn])
                foundRecords.append(row[pkColumn])

                for (columnReturned, columnDescription) in zip(row, cursor.keys()):
                    self.assertEqual(testRow[columnStructure[columnDescription]], columnReturned, msg="Mismatch in '%s':%s Expecting: %s Found: %s" % (row, columnDescription, testRow[columnStructure[columnDescription]], columnReturned))

        self.assertEqual(len(set(matching_pks) ^ set(foundRecords)), 0, msg="Did not find the right matching records: found %s" % (foundRecords))

    def test_connection_check(self):
        s = text('SELECT')
        sqlReturn = self.conn.execute(s)

        self.assertTrue(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
        self.assertEqual(len(sqlReturn.fetchall()), 1)
        sqlReturn.close()

    def test_create_table(self):
        s = text('''
          create table basetable (
          id integer,
          adate date,
          atimestamp timestamp,
          anumeric numeric,
          avarchar varchar
          )
        ''')
        sqlReturn = self.conn.execute(s)
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
        sqlReturn.close()

    @pytest.mark.run(after='test_create_table', before='test_insert_records')
    def test_empty_table(self):
        returnRows = self.conn.execute(text('''SELECT count(*) FROM basetable''')).fetchall()
        self.assertEqual(returnRows[0][0], 0, msg="Was expecting 0 rows got %s" % (returnRows[0][0]))

    @pytest.mark.run(after='test_create_table')
    def test_insert_records(self):
        s = text('''
          insert into basetable (id, adate, atimestamp, anumeric, avarchar) values %s
          ''' % (', '.join([to_sql(entry) for entry in testData])))
        sqlReturn = self.conn.execute(s)
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
        sqlReturn.close()

    @pytest.mark.run(after='test_insert_records')
    def test_loaded_table(self):
        returnRows = self.conn.execute(text('''SELECT count(*) FROM basetable''')).fetchall()
        self.assertEqual(returnRows[0][0], 4, msg="Was expecting 4 rows got %s" % (returnRows[0][0]))
        # numRows = returnRows.scalar()
        # self.assertEqual(numRows, len(testData), msg="Was expecting %s rows got %s" % (len(testData), numRows))

    @pytest.mark.run(after='test_insert_records')
    def test_select_queries(self):
        for query_string, required_rows in query_tests:
            with self.subTest(query_string=query_string, required_rows=required_rows):
                self.query_execute(query_string, required_rows)

    # @pytest.mark.run(after='test_insert_records')
    # def test_select_all(self):
    #     self.query_execute('''SELECT * FROM basetable''', [1, 2, 3, 4])
    #
    # @pytest.mark.run(after='test_insert_records')
    # def test_select_2columns(self):
    #     self.query_execute('''SELECT id,atimestamp FROM basetable''', [1, 2, 3, 4])
    #
    # @pytest.mark.run(after='test_insert_records')
    # def test_select_avarchar_null(self):
    #     self.query_execute('''SELECT * FROM basetable WHERE avarchar IS NULL''', [4])
    #
    # @pytest.mark.run(after='test_insert_records')
    # def test_select_avarchar_not_null(self):
    #     self.query_execute('''SELECT * FROM basetable WHERE avarchar IS NOT NULL''', [1, 2, 3])







    @pytest.mark.run(order=-1)
    def test_delete_table(self):
        sqlReturn = self.conn.execute('DROP TABLE basetable')
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
        sqlReturn.close()



# This is the end of the tests
