import os
import binascii
import csv
import pytest
import pyhs2
from basic_query import TestFDW
from collections import OrderedDict


class TestHiveFDW(TestFDW):
    """
        Test external tables via the Hive FDW
    """

    @pytest.fixture(scope='module')
    def fdw(request):
        """ Which foreign data wrapper are we testing?"""
        return "aeHiveUdv.aeHiveUdv"


    # TODO parameterize this stuff rather than hard code
    @pytest.fixture(scope='module')
    def fdw_options(request):
        """ List of options for the Hive FDW """
        return "host 'localhost', port '10000', authmechanism 'PLAIN', user 'demo', password '', database 'default', table '{for_table_name}'"


    # TODO properly parameterize the parent function so there's no need to rewrite here
    @pytest.fixture(scope='class')
    def foreign_server_function(self, request, connection, multicorn):
        self.exec_no_return(connection, '''
            create or replace function create_foreign_server(fdw_type TEXT) returns void as $block$
              DECLARE
                current_db varchar;
              BEGIN
                SELECT into current_db current_database();
                EXECUTE $$
                CREATE server multicorn_srv foreign data wrapper multicorn options (
                    wrapper '$$ || fdw_type || $$'
                );
                $$;
              END;
            $block$ language plpgsql
            ''')

        def fin():
            self.exec_no_return(connection, '''DROP function create_foreign_server(fdw_type TEXT)''')
        request.addfinalizer(fin)


    @pytest.fixture(scope='class')
    def for_table_columns(self, request):
        """ Schema for the test data in Hive """
        cols = OrderedDict()

        cols['tinyint_a'] = 'TINYINT'
        cols['tinyint_b'] = 'TINYINT'
        cols['smallint_a'] = 'SMALLINT'
        cols['smallint_b'] = 'SMALLINT'
        cols['int_a'] = 'INT'
        cols['int_b'] = 'INT'
        cols['bigint_a'] = 'BIGINT'
        cols['bigint_b'] = 'BIGINT'
        cols['float_a'] = 'FLOAT'
        cols['float_b'] = 'FLOAT'
        cols['double_a'] = 'DOUBLE'
        cols['double_b'] = 'DOUBLE'
        cols['decimal_a'] = 'DECIMAL(20,10)'
        cols['decimal_b'] = 'DECIMAL(20,10)'
        cols['timestamp_a'] = 'TIMESTAMP'
        cols['timestamp_b'] = 'TIMESTAMP'
        cols['date_a'] = 'DATE'
        cols['date_b'] = 'DATE'
        cols['string_a'] = 'STRING'
        cols['string_b'] = 'STRING'
        cols['varchar_a'] = 'VARCHAR(100)'
        cols['varchar_b'] = 'VARCHAR(100)'
        cols['char_a'] = 'CHAR(100)'
        cols['char_b'] = 'CHAR(100)'
        cols['boolean_a'] = 'BOOLEAN'
        cols['boolean_b'] = 'BOOLEAN'
        cols['binary_a'] = 'BINARY'
        cols['binary_b'] = 'BINARY'

        return cols


    def import_hive_schema(self,for_table_columns):

        # TODO again this should probably be parameterized
        # connect to Hive
        conn = pyhs2.connect(
            host='127.0.0.1',
            port = 10000,
            authMechanism='PLAIN',
            user='demo',
            password='',
            database='default')

        # convert schema dicts to SQL schema
        external_table_schema = ', '.join(['%s %s' % (k,v) for k,v in for_table_columns.iteritems()])
        view_schema = ", ".join(['cast (%s AS %s) AS %s' % (k,v,k) for k,v in for_table_columns.iteritems()])

        # set up external table and view in Hive
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS query_for_ext')
            cur.execute("""
                CREATE EXTERNAL TABLE IF NOT EXISTS query_for_ext (
                %s
                )
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                    WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\\"", "escapeChar"="\\\\")
                    STORED AS TEXTFILE
                    LOCATION '/udvtest'
                    TBLPROPERTIES ("skip.header.line.count"="1")
                """ % (external_table_schema)
            )

            cur.execute('DROP VIEW IF EXISTS query_for')
            cur.execute("""
                CREATE VIEW IF NOT EXISTS query_for AS SELECT
                %s
                   FROM query_for_ext
                """ % (view_schema)
            )


    def import_hive_data(self, for_table_columns):

        # create a temp csv in the right format for hive from test_data
        # TODO break this out into a separate function, (same for the stuff above)
        noneValue = '<None>'
        booleanMap = { 't': 'TRUE', 'f': 'FALSE', '': ''}
        cols = for_table_columns.keys()
        with self.sample_io() as csvfile:
            with open('/tmp/hive_test_data.csv', 'w') as hivefile:
                csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
                csvwriter = csv.writer(hivefile)
                csvwriter.writerow(cols)
                for row in csvreader:
                    actualRow = {k: v.replace('<None>', '') for k,v in row.iteritems()}
                    actualRow['boolean_a'] = booleanMap[actualRow['boolean_a']]
                    actualRow['boolean_b'] = booleanMap[actualRow['boolean_b']]
                    actualRow['binary_a'] = binascii.unhexlify(actualRow['binary_a'][2:])
                    actualRow['binary_b'] = binascii.unhexlify(actualRow['binary_b'][2:])
                    csvwriter.writerow([actualRow[x] for x in cols])

        # add test_data to hdfs
        os.system('sudo su hdfs -c "hdfs dfs -copyFromLocal -f /tmp/hive_test_data.csv /udvtest"')


    @pytest.fixture(scope='class')
    def for_table_populated(self, request, connection, ref_table, for_table_columns):
        """ Class to import schema into Hive, and load the test data"""

        self.import_hive_schema(for_table_columns)

        self.import_hive_data(for_table_columns)

        def fin():
            # drop the table schemas
            with conn.cursor() as cur:
                cur.execute('DROP TABLE IF EXISTS query_for_ext')
                cur.execute('DROP VIEW IF EXISTS query_for')
            conn.close()

            # remove files from hdfs
            os.system('sudo su hdfs -c "hdfs dfs -rm /udvtest/*"')

        request.addfinalizer(fin)
