import warnings
import pytest
import collections
import csv
from operator import itemgetter
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import Table, MetaData


class MulticornBaseTest:


    # TODO: Make this into a property
    @classmethod
    def ref_table_name(cls):
        return 'query_ref'

    # TODO: Make this into a property
    @classmethod
    def for_table_name(cls):
        return 'query_for'


    # ==========================================================================
    #  Database connection fixtures
    # ==========================================================================

    @pytest.fixture(scope="module")
    def db_engine(self, request, username, password, db):
        """Fixture for creating a database engine.  Requires username, password and db fixtures to be added."""
        print "Creating SQLAlchemy DB engine"
        engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (username, password, db), echo=True, poolclass=NullPool)

        def fin():
            print "Removing SQLAlchemy DB engine"
            engine.dispose()
        request.addfinalizer(fin)

        return engine


    @pytest.fixture(scope="module")
    def connection(self, request, db_engine):
        """Fixture for connecting to the database"""
        print "Opening DB connection"
        conn = db_engine.connect()

        def fin():
            conn.close()
            db_engine.dispose()
            print "Closed DB connection"
        request.addfinalizer(fin)

        return conn


    @pytest.fixture(scope="module")
    def metadata(self, request, db_engine, connection):
        """Fixture for creating metadata for the database"""
        metadata = MetaData(bind=db_engine)
        return metadata


    # ==========================================================================
    #  Reference table fixtures
    # ==========================================================================

    @pytest.fixture(scope="class")
    def ref_table(self, request, connection, db_engine, metadata, table_columns):
        """ Fixture for creating a reference table.
            This is a native PostgreSQL table, against which the results from the foreign table are compared
        """

        define_cols = ", ".join(["%s %s" % (k,v) for k,v in table_columns.iteritems()])
        self.exec_sql(connection, '''CREATE TABLE {0} ( {1} );'''.format(self.ref_table_name(), define_cols))
        # add table to metadata (this allows use of the insert api later on)
        ref_table = Table(self.ref_table_name(), metadata, autoload=True, autoload_with=db_engine)

        def fin():
            metadata.remove(ref_table)
            self.exec_sql(connection, 'DROP TABLE {0}'.format(self.ref_table_name()))
        request.addfinalizer(fin)

        return ref_table


    @pytest.fixture(scope="class")
    def ref_table_populated(self, request, connection, ref_table):
        """Fixture for creating a populated reference table"""

        noneValue = '<None>'
        with self.sample_io() as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
            for row in spamreader:
                # Fake a NULL into the CSV as python CSV does not support Null entries
                # http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string
                actualRow = {item[0]: item[1] for item in row.items() if item[1] != noneValue}
                temp = ref_table.insert().values(**actualRow)
                connection.execute(temp)
        #assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)

        def fin():
            self.exec_sql(connection, 'DELETE FROM {0}'.format(self.ref_table_name()))

        request.addfinalizer(fin)

    def test_ref_table_populated(self, connection, ref_table_populated):
        (keys, values) = self.exec_return_value(connection, 'SELECT * FROM {0}'.format(self.ref_table_name()))
        assert len(values) > 0, 'Expecting %s to have data, found %s' % (self.ref_table_name(), values)


    # ==========================================================================
    #  Foreign table fixtures
    # ==========================================================================

    @pytest.fixture(scope='class')
    def multicorn(self, request, connection):
        """Fixture for creating multicorn extension"""

        self.exec_no_return(connection, '''CREATE EXTENSION IF NOT EXISTS multicorn''')

        def fin():
            self.exec_no_return(connection, '''DROP EXTENSION multicorn''')
        request.addfinalizer(fin)

        return None

    def test_multicorn(self, connection, multicorn):
        (keys, values) = self.exec_return_value(connection, "SELECT * FROM pg_catalog.pg_extension WHERE extname='multicorn'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)


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
                    wrapper '$$ || fdw_type || $$',
                    db_url 'postgresql://$$ || current_user || '@localhost/' || current_db || $$'
                );
                $$;
              END;
            $block$ language plpgsql
            ''')

        def fin():
            self.exec_no_return(connection, '''DROP function create_foreign_server(fdw_type TEXT)''')
        request.addfinalizer(fin)
        return None

    def test_foreign_server_function(self, connection, foreign_server_function):
        (keys, values) = self.exec_return_value(connection, "SELECT * FROM information_schema.routines WHERE routine_type='FUNCTION' AND specific_schema='public' AND routine_name='create_foreign_server'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)


    @pytest.fixture(scope='class')
    def foreign_server(self, request, connection, foreign_server_function, fdw):
        """Fixture to create the foreign server for connecting to external database via psql """

        (keys, values) = self.exec_return_value(connection, '''SELECT create_foreign_server('{0}')'''.format(fdw))
        assert 1, "Do not care about return keys or values"

        def fin():
            self.exec_no_return(connection, '''DROP SERVER multicorn_srv''')
        request.addfinalizer(fin)

        return None

    def test_foreign_server(self, connection, foreign_server):
        (keys, values) = self.exec_return_value(connection, "SELECT * FROM information_schema.foreign_servers WHERE foreign_server_name='multicorn_srv'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)


    @pytest.fixture(scope='class')
    def foreign_table(self, request, connection, ref_table, foreign_server, password, fdw_options, table_columns):
        """Fixture to create the foreign table we will test against.  Defaults to pointing at the ref table via the PostgreSQL FDW"""

        print("Looking at fdw_options:%s" % (fdw_options))
        fdw_options_expanded = fdw_options.format(
            for_table_name=self.for_table_name(),
            ref_table_name=self.ref_table_name(),
            password=password,
            )

        define_cols = ", ".join(["%s %s" % (k,v) for k,v in table_columns.iteritems()])
        self.exec_no_return(connection, '''
            create foreign table {for_table_name} (
                {columns}
            ) server multicorn_srv options (
                {fdw_options}
            )
            '''.format(for_table_name=self.for_table_name(), columns=define_cols, fdw_options=fdw_options_expanded))

        def fin():
            self.exec_no_return(connection, '''DROP FOREIGN TABLE {for_table_name}'''.format(for_table_name=self.for_table_name()))
        request.addfinalizer(fin)

        return None

    def test_foreign_table(self, connection, foreign_table):
        (keys, values) = self.exec_return_value(connection, "SELECT * FROM information_schema.foreign_tables WHERE foreign_table_name='{0}'".format(self.for_table_name()))
        assert len(values) == 1, 'Expecting one record got %s' % (values)


    @pytest.fixture(scope="class")
    def for_table_populated(self, request, connection, ref_table):
        """
            Fixture to populate the foreign table with data.  By default does nothing (for table points at ref table, which is already populated)
            Should be overriden in child class. The fixture should:
              - connect to the external database
              - set up the test table (if necessary)
              - populate it with the same data as the ref table
              - clean up and remove the table and add finalizer
              - doesn't need to return anything
        """
        print("Populate for_table")

        def fin():
            print("Cleanup for_table")

        request.addfinalizer(fin)


    # ==========================================================================
    #  Utility Methods
    # ==========================================================================

    def exec_sql(self, connection, query):

        # TODO there's still a connection leak issue, this code can be used to debug by greping output for 'mattout'
        #conn_count = connection.execute('SELECT sum(numbackends) FROM pg_stat_database;')
        #conn_count = conn_count.fetchall()
        #print 'mattout running query %s, %s connections' % (query,conn_count[0][0])

        sqlReturn = connection.execute(query)
        return sqlReturn

    def exec_no_return(self, connection, query):
        returnVal = self.exec_sql(connection, query)
        assert not returnVal.returns_rows, "Not expecting any rows"

    def exec_return_empty(self, connection, query):
        returnVal = self.exec_sql(connection, query)
        assert returnVal.returns_rows, "Expecting rows"
        assert returnVal.rowcount == 1, "Expecting a single row"
        assert len(returnVal.keys()) == 0, "Should not return any columns, found %s" % (returnVal.keys())

    def exec_return_value(self, connection, query):
        returnVal = self.exec_sql(connection, query)
        assert returnVal.returns_rows, "Expecting rows"
        return (returnVal.keys(), returnVal.fetchall())

    # isclose is a built in function for python 3
    @staticmethod
    def isclose_double (a, b, rel_tol=1e-09, abs_tol=0.0):
        difference = abs (a-b)
        tolerance  = max (rel_tol * max (abs (a), abs (b)), abs_tol)
        return (difference <= tolerance, difference, tolerance)

    # isclose is a built in function for python 3
    @staticmethod
    def isclose_float (a, b, rel_tol=1e-05, abs_tol=0.0):
        difference = abs (a-b)
        tolerance  = max (rel_tol * max (abs (a), abs (b)), abs_tol)
        return (difference <= tolerance, difference, tolerance)

    def unordered_query(self, connection, query):
        query_ref = query.format(table_name=self.ref_table_name())
        query_for = query.format(table_name=self.for_table_name())

        return_ref = self.exec_sql(connection, query_ref)
        return_for = self.exec_sql(connection, query_for)

        # failure when one query returns rows and one does not return rows
        assert return_ref.returns_rows == return_for.returns_rows, "Expecting ref and for to have matching returns_rows"

        # success if no rows to process
        if not return_ref.returns_rows and not return_for.returns_rows:
            return

        # failure if number of rows does not match 
        assert return_ref.rowcount == return_for.rowcount, "Expecting ref and for to have same number of returning rows"

        # failure if columns do not match
        assert collections.Counter (return_ref.keys ()) == collections.Counter (return_for.keys ()), "Expecting ref and for to have the same returning columns"
        keys_all = return_ref.keys () 

        # convert data to lists of dictionaries
        data_ref = return_ref.fetchall ()
        data_for = return_for.fetchall ()
        print ('Checking match of ref:%s == for:%s' % (data_ref, data_for))

        data_ref_unsorted = []
        for rows_ref in data_ref:
            items_ref = dict (rows_ref.items ())
            data_ref_unsorted.append (items_ref)
        data_for_unsorted = []
        for rows_for in data_for:
            items_for = dict (rows_for.items ())
            data_for_unsorted.append (items_for)

        # test for data with an id column
        if u"id" in return_ref.keys () and u"id" in return_for.keys ():
            # sort the list of dictionaries by 'id' column
            data_ref_sorted = sorted (data_ref_unsorted, key=itemgetter ('id'))
            data_for_sorted = sorted (data_for_unsorted, key=itemgetter ('id'))
            for rows_ref,rows_for in zip (data_ref_sorted, data_for_sorted):
                items_ref = dict (rows_ref.items ())
                items_for = dict (rows_for.items ())
                for key in keys_all:
                    # float comparison
                    if isinstance (items_ref[key], float):
                        # if not identical, check for close result
                        if items_ref[key] != items_for[key]:
                            __result,__difference,__tolerance = MulticornBaseTest.isclose_float (items_ref[key], items_for[key])
                            assert __result, "Expecting results from both queries to be within tolerance apart from order (%s) ... ref[%s] (almost)== for[%s]: %s (almost)== %s ... difference: %s, tolerance: %s" \
                                    % (type (items_ref[key]), key, key, items_ref[key], items_for[key], __difference, __tolerance)
                            # close result, issue warning
                            with pytest.warns (UserWarning):
                                warnings.warn ("Expected results not identical but within tolerance (%s) ... ref[%s] (almost)== for[%s]: %s (almost)== %s ... difference: %s, tolerance %s" \
                                        % (type (items_ref[key]), key, key, items_ref[key], items_for[key], __difference, __tolerance), UserWarning)
                    # all other type comparisons
                    else:
                        assert str (items_ref[key]) == str (items_for[key]), "Expecting results from both queries to be identical apart from order (%s) ... ref[%s] == for[%s]: %s == %s" \
                                % (type (items_ref[key]), key, key, items_ref[key], items_for[key])
            return

        # test for data without an id column
        # convert all data types to strings for equality comparison
        data_ref_processed = []
        data_for_processed = []
        for rows_ref,rows_for in zip (data_ref_unsorted, data_for_unsorted):
            items_ref_processed = []
            items_for_processed = []
            for key in keys_all:
                items_ref_processed.append (str (rows_ref[key]))
                items_for_processed.append (str (rows_for[key]))
            data_ref_processed.append (",".join (items_ref_processed))
            data_for_processed.append (",".join (items_for_processed))
        assert collections.Counter(data_ref_processed) == collections.Counter(data_for_processed), 'Expecting results from both queries to be identical apart from order'

    def ordered_query(self, connection, query):
        query_ref = query.format(table_name=self.ref_table_name())
        query_for = query.format(table_name=self.for_table_name())

        return_ref = self.exec_sql(connection, query_ref)
        return_for = self.exec_sql(connection, query_for)

        # failure when one query returns rows and one does not return rows
        assert return_ref.returns_rows == return_for.returns_rows, "Expecting ref and for to have matching returns_rows"

        # success if no rows to process
        if not return_ref.returns_rows and not return_for.returns_rows:
            return

        # failure if number of rows does not match 
        assert return_ref.rowcount == return_for.rowcount, "Expecting ref and for to have same number of returning rows"

        # failure if columns do not match
        assert collections.Counter (return_ref.keys ()) == collections.Counter (return_for.keys ()), "Expecting ref and for to have the same returning columns"
        keys_all = return_ref.keys ()

        # convert data to lists of dictionaries
        data_ref = return_ref.fetchall ()
        data_for = return_for.fetchall ()
        print ('Checking match of ref:%s == for:%s' % (data_ref, data_for))

        data_ref_unsorted = []
        for rows_ref in data_ref:
            items_ref = dict (rows_ref.items ())
            data_ref_unsorted.append (items_ref)
        data_for_unsorted = []
        for rows_for in data_for:
            items_for = dict (rows_for.items ())
            data_for_unsorted.append (items_for)

        # convert all data types to strings for equality comparison
        data_ref_processed = []
        data_for_processed = []
        for rows_ref,rows_for in zip (data_ref_unsorted, data_for_unsorted):
            items_ref_processed = []
            items_for_processed = []
            for key in keys_all:
                items_ref_processed.append (str (rows_ref[key]))
                items_for_processed.append (str (rows_for[key]))
            data_ref_processed.append (",".join (items_ref_processed))
            data_for_processed.append (",".join (items_for_processed))

        for (row_ref, row_for) in zip (data_ref_processed, data_for_processed):
            print ('Checking match of ref:%s == for:%s' % (row_ref, row_for))
            assert row_ref == row_for, 'Rows should match %s == %s' % (row_ref, row_for)

