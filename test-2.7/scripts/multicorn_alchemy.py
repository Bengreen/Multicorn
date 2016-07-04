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
#
# from sqlalchemy import create_engine
# from sqlalchemy import Table, Column, Integer, String, MetaData, Date, DateTime, Numeric
# from sqlalchemy.orm import mapper, sessionmaker
# import datetime
#
#
# engine = create_engine('sqlite:///:memory:', echo=True)
# metadata = MetaData()
#
# testdata_table = Table(
#         'testdata', metadata,
#         Column('id', Integer, primary_key=True),
#         Column('adate', Date),
#         Column('atimestamp', DateTime),
#         Column('anumeric', Numeric),
#         Column('avarchar', String)
#         )
# metadata.create_all(engine)
#
# setattr and getattr
#
# class Testdata(object):
#     def __init__(self, id, adate, atimestamp, anumeric, avarchar):
#         self.id = id
#         self.adate = adate
#         self.atimestamp = atimestamp
#         self.anumeric = anumeric
#         self.avarchar = avarchar
#
#     def __repr__(self):
#         return "Testdata(%s)" % (self.id)
#
# mapper(Testdata, testdata_table)
#
# a_testpoint = Testdata(1, datetime.datetime.strptime('1980-01-01', '%Y-%m-%d').date(), datetime.datetime.strptime('1980-01-01  11:01:21.132912', '%Y-%m-%d  %H:%M:%S.%f'), 3.4, 'Test')
# a_testpoint
# a_testpoint.adate
#
# Session = sessionmaker(bind=engine)
# session = Session()
#
# session.add(a_testpoint)
# session.commit()


testDataSrc = [
    (1, '1980-01-01', '1980-01-01  11:01:21.132912', 3.4, 'Test'),
    (2, '1990-03-05', '1998-03-02  10:40:18.321023', 12.2, 'Another Test'),
    (3, '1972-01-02', '1972-01-02  16:12:54.000000', 4000, 'another Test'),
    (4, '1922-11-02', '1962-01-02  23:12:54.000000', -3000, None)]


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
    ('''SELECT * FROM {0}''', {1, 2, 3, 4}),
    ('''SELECT id,atimestamp FROM {0}''', {1, 2, 3, 4}),
    ('''SELECT * FROM {0} WHERE avarchar IS NULL''', {4}),
    ('''SELECT * FROM {0} WHERE avarchar IS NOT NULL''', {1, 2, 3}),
    ('''SELECT * from {0} where adate > '1970-01-02'::date''', {1, 2, 3}),
    ('''SELECT * from {0} where adate between '1970-01-01' and '1980-01-01' ''', {1, 3}),
    ('''SELECT * from {0} where anumeric > 0''', {1, 2, 3}),
    ('''SELECT * from {0} where avarchar not like '%%test' ''', {1, 2, 3}),
    ('''SELECT * from {0} where avarchar like 'Another%%' ''', {2}),
    ('''SELECT * from {0} where avarchar ilike 'Another%%' ''', {2, 3}),
    ('''SELECT * from {0} where avarchar not ilike 'Another%%' ''', {1}),
    ('''SELECT * from {0} where id in (1,2)''', {1, 2}),
    ('''SELECT * from {0} where id not in (1, 2)''', {3, 4}),
    ('''SELECT * from {0} order by avarchar''', [3, 2, 1, 4]),
    ('''SELECT * from {0} order by avarchar desc''', [4, 1, 2, 3]),
    ('''SELECT * from {0} order by avarchar desc nulls first''', [4, 1, 2, 3]),
    ('''SELECT * from {0} order by avarchar desc nulls last''', [1, 2, 3, 4]),
    ('''SELECT * from {0} order by avarchar nulls first''', [4, 3, 2, 1]),
    ('''SELECT * from {0} order by avarchar nulls last''', [3, 2, 1, 4]),
    ('''SELECT count(*) FROM {0}''', 4),
]


def findRow(pk):
    return [row for row in testData if row[0] == pk][0]


@pytest.mark.usefixtures("params")
class TestConnectionToPostgres(unittest.TestCase):
    '''
    Create the test framework for running tests on posgresql
    '''

    @classmethod
    def setUpClass(cls):
        print 'Creating new engine for DB'
        cls.engine = None
        # cls.engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (username, password, db), echo=True)

    @classmethod
    def tearDownClass(cls):
        print 'Releasing DB (TODO)'

    def setUp(self):
        if not self.engine:
            print 'Creating new engine for DB'
            self.engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (self.username, self.password, self.db), echo=True)

        print "Creating new DB connection"
        self.conn = self.engine.connect()
        # Need to force a transaction and subsequent commit as sqlalchemy 'cleverly detects' commit type lanugage and auto triggers commits. But fails to see executing functions using SELECT as requiring a commit()
        self.trans = self.conn.begin()

    def tearDown(self):
        self.trans.commit()
        self.conn.close()
        print 'Released DB connection'

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
        s = text('SELECT')
        sqlReturn = self.conn.execute(s)

        self.assertTrue(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
        self.assertEqual(len(sqlReturn.fetchall()), 1)

    @pytest.mark.run(order=1)
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

    @pytest.mark.run(order=2)
    def test_count_records_empty(self):
        returnRows = self.conn.execute(text('''SELECT count(*) FROM basetable''')).fetchall()
        self.assertEqual(returnRows[0][0], 0, msg="Was expecting 0 rows got %s" % (returnRows[0][0]))

    @pytest.mark.run(order=2)
    def test_load_records(self):
        s = text('''
          insert into basetable (id, adate, atimestamp, anumeric, avarchar) values %s
          ''' % (', '.join([to_sql(entry) for entry in testData])))
        sqlReturn = self.conn.execute(s)
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))

    @pytest.mark.run(order=3)
    def test_count_records(self):
        returnRows = self.conn.execute(text('''SELECT count(*) FROM basetable''')).fetchall()
        self.assertEqual(returnRows[0][0], 4, msg="Was expecting 4 rows got %s" % (returnRows[0][0]))

    @pytest.mark.run(order=4)
    def test_select_queries_basetable(self):
        table_name = 'basetable'
        for query_string, required_rows in query_tests:
            with self.subTest(table_name=table_name, query_string=query_string, required_rows=required_rows):
                self.query_execute(table_name, query_string, required_rows)

    @pytest.mark.run(order=10)
    def test_create_extension_multicorn(self):
        sqlReturn = self.conn.execute('''CREATE EXTENSION multicorn''')

    @pytest.mark.run(order=15)
    def test_create_helper_function(self):
        sqlReturn = self.conn.execute('''
            create or replace function create_foreign_server() returns void as $block$
              DECLARE
                current_db varchar;
              BEGIN
                SELECT into current_db current_database();
                EXECUTE $$
                CREATE server multicorn_srv foreign data wrapper multicorn options (
                    wrapper 'multicorn.sqlalchemyfdw.SqlAlchemyFdw',
                    db_url 'postgresql://$$ || current_user || '@localhost/' || current_db || $$'
                );
                $$;
              END;
            $block$ language plpgsql
            ''')

    @pytest.mark.run(order=20)
    def test_create_foreign_server(self):
        sqlReturn = self.conn.execute('''SELECT create_foreign_server()''')

    @pytest.mark.run(order=23)
    def test_check_foreign_server(self):
        sqlReturn = self.conn.execute('''select * from pg_foreign_server WHERE srvname='multicorn_srv' ''')
        all_rows = sqlReturn.fetchall()
        self.assertEqual(len(all_rows), 1, msg="There should be 1 row with name multicorn_srv")

    @pytest.mark.run(order=25)
    def test_create_foreign_table(self):
        sqlReturn = self.conn.execute('''
            create foreign table testalchemy (
              id integer,
              adate date,
              atimestamp timestamp,
              anumeric numeric,
              avarchar varchar
            ) server multicorn_srv options (
              tablename 'basetable',
              password '%s'
            )
            ''' % (self.password))

    @pytest.mark.run(order=30)
    def test_select_queries_testalchemy(self):
        table_name = 'testalchemy'
        for query_string, required_rows in query_tests:
            with self.subTest(table_name=table_name, query_string=query_string, required_rows=required_rows):
                self.query_execute(table_name, query_string, required_rows)

    @pytest.mark.run(order=-10)
    def test_delete_foreign_table(self):
        sqlReturn = self.conn.execute('DROP FOREIGN TABLE testalchemy')
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))

    @pytest.mark.run(order=-7)
    def test_delete_foreign_server(self):
        sqlReturn = self.conn.execute('DROP SERVER multicorn_srv')
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))

    @pytest.mark.run(order=-5)
    def test_drop_helper_function(self):
        sqlReturn = self.conn.execute('''DROP function create_foreign_server()''')

    @pytest.mark.run(order=-3)
    def test_drop_extension_multicorn(self):
        sqlReturn = self.conn.execute('''DROP EXTENSION multicorn''')

    @pytest.mark.run(order=-1)
    def test_delete_table(self):
        sqlReturn = self.conn.execute('DROP TABLE basetable')
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))

# This is the end of the tests
