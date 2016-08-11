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
#        return "host 'localhost', port '10000', authmechanism 'PLAIN', user 'demo', password '', database 'default', table '{for_table_name}'"
        return "host 'localhost', port '10000', authmechanism 'PLAIN', user 'hdfs', password '', database 'default', table '{for_table_name}'"


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
#        conn = pyhs2.connect(
#            host='127.0.0.1',
#            port = 10000,
#            authMechanism='PLAIN',
#            user='demo',
#            password='',
#            database='default')
        conn = pyhs2.connect(
            host='127.0.0.1',
            port = 10000,
            authMechanism='PLAIN',
            user='hdfs',
            password='',
            database='default')

        # convert schema dicts to SQL schema
        external_table_schema = ', '.join(['%s %s' % (k,v) for k,v in for_table_columns.iteritems()])
        # hack to allow null boolean and binary types in a hive view (should be documented for users)
#        view_schema = ", ".join(['cast (%s AS %s) AS %s' % (k,v,k) for k,v in for_table_columns.iteritems() if not any([t in k for t in ['boolean', 'binary']])])
#        view_schema += ", CASE boolean_a WHEN 'TRUE' THEN TRUE WHEN 'FALSE' THEN FALSE ELSE NULL END AS boolean_a,"
#        view_schema += " CASE boolean_b WHEN 'TRUE' THEN TRUE WHEN 'FALSE' THEN FALSE ELSE NULL END AS boolean_b,"
#        view_schema += " CASE binary_a WHEN '' THEN NULL ELSE binary_a END AS binary_a,"
#        view_schema += " CASE binary_b WHEN '' THEN NULL ELSE binary_b END AS binary_b"
        view_schema = """cast (tinyint_a AS TINYINT)        AS tinyint_a,
cast (tinyint_b AS TINYINT)        AS tinyint_b,
cast (smallint_a AS SMALLINT)      AS smallint_a,
cast (smallint_b AS SMALLINT)      AS smallint_b,
cast (int_a AS INT)                AS int_a,
cast (int_b AS INT)                AS int_b,
cast (bigint_a AS BIGINT)          AS bigint_a,
cast (bigint_b AS BIGINT)          AS bigint_b,
cast (float_a AS FLOAT)            AS float_a,
cast (float_b AS FLOAT)            AS float_b,
cast (double_a AS DOUBLE)          AS double_a,
cast (double_b AS DOUBLE)          AS double_b,
cast (decimal_a AS DECIMAL(20,10)) AS decimal_a,
cast (decimal_b AS DECIMAL(20,10)) AS decimal_b,
cast (timestamp_a AS TIMESTAMP)    AS timestamp_a,
cast (timestamp_b AS TIMESTAMP)    AS timestamp_b,
cast (date_a AS DATE)              AS date_a,
cast (date_b AS DATE)              AS date_b,
cast (CASE string_a WHEN '<None>' THEN NULL ELSE string_a END AS STRING) AS string_a,
cast (CASE string_b WHEN '<None>' THEN NULL ELSE string_b END AS STRING) AS string_b,
cast (CASE varchar_a WHEN '<None>' THEN NULL ELSE varchar_a END AS VARCHAR(100)) AS varchar_a,
cast (CASE varchar_b WHEN '<None>' THEN NULL ELSE varchar_b END AS VARCHAR(100)) AS varchar_b,
cast (CASE char_a WHEN '<None>' THEN NULL ELSE char_a END AS CHAR(100)) AS char_a,
cast (CASE char_b WHEN '<None>' THEN NULL ELSE char_b END AS CHAR(100)) AS char_b,
cast (CASE boolean_a WHEN '<None>' THEN NULL WHEN 't' THEN TRUE WHEN 'f' THEN FALSE END AS BOOLEAN) AS boolean_a,
cast (CASE boolean_b WHEN '<None>' THEN NULL WHEN 't' THEN TRUE WHEN 'f' THEN FALSE END AS BOOLEAN) AS boolean_b,
cast (CASE binary_a WHEN '<None>' THEN NULL ELSE binary_a END AS BINARY) AS binary_a,
cast (CASE binary_b WHEN '<None>' THEN NULL ELSE binary_b END AS BINARY) AS binary_b"""

        # set up external table and view in Hive
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS query_for_ext')
#            cur.execute("""
#                CREATE EXTERNAL TABLE IF NOT EXISTS query_for_ext (
#                %s
#                )
#                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
#                    WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\\"", "escapeChar"="\\\\")
#                    STORED AS TEXTFILE
#                    LOCATION '/udvtest'
#                    TBLPROPERTIES ("skip.header.line.count"="1")
#                """ % (external_table_schema)
#            )
            cur.execute("""
                CREATE EXTERNAL TABLE IF NOT EXISTS query_for_ext (
                %s
                )
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                    WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\\"", "escapeChar"="~")
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
        noneValue = '<None>'
        booleanMap = { 't': 'TRUE', 'f': 'FALSE', '': 'NULL'}
        cols = for_table_columns.keys()
        with self.sample_io() as csvfile:
            with open('/tmp/hive_test_data.csv', 'w') as hivefile:
                csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
                csvwriter = csv.writer(hivefile)
                csvwriter.writerow(cols)
                for row in csvreader:
#                    actualRow = {k: v.replace('<None>', '') for k,v in row.iteritems()}
#                    actualRow['boolean_a'] = booleanMap[actualRow['boolean_a']]
#                    actualRow['boolean_b'] = booleanMap[actualRow['boolean_b']]
#                    actualRow['binary_a'] = binascii.unhexlify(actualRow['binary_a'][2:])
#                    actualRow['binary_b'] = binascii.unhexlify(actualRow['binary_b'][2:])
                    actualRow = {k: v.replace('<None>', '<None>') for k,v in row.iteritems()}
#                    actualRow['binary_a'] = binascii.unhexlify(actualRow['binary_a'][2:]) if actualRow['binary_a'] != '<None>' else '<None>'
#                    actualRow['binary_b'] = binascii.unhexlify(actualRow['binary_b'][2:]) if actualRow['binary_b'] != '<None>' else '<None>'
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
#            with conn.cursor() as cur:
#                cur.execute('DROP TABLE IF EXISTS query_for_ext')
#                cur.execute('DROP VIEW IF EXISTS query_for')
#            conn.close()

            # remove files from hdfs
            os.system('sudo su hdfs -c "hdfs dfs -rm /udvtest/*"')
#            pass

        request.addfinalizer(fin)
