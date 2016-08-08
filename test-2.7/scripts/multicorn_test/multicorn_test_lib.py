import pytest
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
import collections

import csv

from sqlalchemy.ext.declarative import declarative_base

# TODO: Move this into the class (if possible)
Base = declarative_base()


class MulticornBaseTest:
    # Create fixture abstract for executing Foreign table setup
    @pytest.fixture(scope="class")
    def for_table_populated(self, request, session_factory, ref_table):
        print("Populate for_table")

        def fin():
            print("Cleanup for_table")

        request.addfinalizer(fin)
        return  # provide the fixture value

    # TODO: Make this into a property
    @classmethod
    def ref_table_name(cls):
        return 'query_ref'

    # TODO: Make this into a property
    @classmethod
    def for_table_name(cls):
        return 'query_for'

    @pytest.fixture(scope="module")
    def db_engine(self, request, username, password, db):
        print("Connecting to PG Engine")
        engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (username, password, db), echo=True, poolclass=NullPool)

        def fin():
            engine.dispose()
            print("Closed PG Engine")

        request.addfinalizer(fin)
        return engine  # provide the fixture value

    @pytest.fixture(scope="module")
    def session_factory(self, request, db_engine):
        print("Creating and binding Session factory")
        Session = sessionmaker(bind=db_engine, autoflush=False)

        return Session  # provide the fixture value

    def test_session_factory(self, session_factory):
        self.exec_return_empty(session_factory, 'SELECT')

    @pytest.fixture(scope="class")
    def ref_table(self, request, session_factory, db_engine, table_columns):
        self.exec_return_no_rows(session_factory, '''CREATE TABLE {0} ({1})'''.format(self.ref_table_name(), table_columns))

        Base.metadata.reflect(bind=db_engine, only=[self.ref_table_name()])

        ref_table = Base.metadata.tables[self.ref_table_name()]

        def fin():
            Base.metadata.remove(ref_table)
            self.exec_return_no_rows(session_factory, 'DROP TABLE {0}'.format(self.ref_table_name()))

        request.addfinalizer(fin)

        return ref_table

    def test_ref_table(self, session_factory, ref_table):
        values = self.exec_return_values(session_factory, 'SELECT * FROM {0}'.format(self.ref_table_name()))
        assert len(values) == 0, 'Expecting %s to be empty, found %s' % (self.ref_table_name(), values)

    @pytest.fixture(scope="function")
    def ref_table_populated(self, request, session_factory, ref_table):
        session = session_factory()
        noneValue = '<None>'
        with self.sample_io() as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
            for row in spamreader:
                # Fake a NULL into the CSV as python CSV does not support Null entries
                # http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string
                actualRow = {item[0]: item[1] for item in row.items() if item[1] != noneValue}

                temp = ref_table.insert().values(**actualRow)
                session.execute(temp)
        assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)
        session.commit()
        session.close()

        def fin():
            self.exec_return_no_rows(session_factory, 'DELETE FROM {0}'.format(self.ref_table_name()))

        request.addfinalizer(fin)

    def test_ref_table_populated(self, session_factory, ref_table_populated):
        values = self.exec_return_values(session_factory, 'SELECT * FROM {0}'.format(self.ref_table_name()))
        assert len(values) > 0, 'Expecting %s to have data, found %s' % (self.ref_table_name(), values)

    @pytest.fixture(scope='function')
    def multicorn(self, request, session_factory):
        self.exec_return_no_rows(session_factory, '''CREATE EXTENSION multicorn''')

        def fin():
            self.exec_return_no_rows(session_factory, '''DROP EXTENSION multicorn''')

        request.addfinalizer(fin)
        return None

    def test_multicorn(self, session_factory, multicorn):
        values = self.exec_return_values(session_factory, "SELECT * FROM pg_catalog.pg_extension WHERE extname='multicorn'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture(scope='function')
    def helper_function(self, request, session_factory, multicorn):
        self.exec_return_no_rows(session_factory, '''
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
            self.exec_return_no_rows(session_factory, '''DROP function create_foreign_server(fdw_type TEXT)''')
        request.addfinalizer(fin)
        return None

    def test_helper_function(self, session_factory, helper_function):
        values = self.exec_return_values(session_factory, "SELECT * FROM information_schema.routines WHERE routine_type='FUNCTION' AND specific_schema='public' AND routine_name='create_foreign_server'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture(scope='function')
    def foreign_server(self, request, session_factory, helper_function, fdw):
        values = self.exec_return_values(session_factory, '''SELECT create_foreign_server('{0}')'''.format(fdw))
        assert 1, "Do not care about return keys or values"

        def fin():
            self.exec_return_no_rows(session_factory, '''DROP SERVER multicorn_srv''')
        request.addfinalizer(fin)
        return None

    def test_foreign_server(self, session_factory, foreign_server):
        values = self.exec_return_values(session_factory, "SELECT * FROM information_schema.foreign_servers WHERE foreign_server_name='multicorn_srv'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture
    def foreign_table(self, request, session_factory, ref_table, foreign_server, password, fdw_options, table_columns):
        print("Looking at fdw_options:%s" % (fdw_options))
        fdw_options_expanded = fdw_options.format(
            for_table_name=self.for_table_name(),
            ref_table_name=self.ref_table_name(),
            password=password,
            )

        self.exec_return_no_rows(session_factory, '''
            create foreign table {for_table_name} (
                {columns}
            ) server multicorn_srv options (
                {fdw_options}
            )
            '''.format(for_table_name=self.for_table_name(), columns=table_columns, fdw_options=fdw_options_expanded))

        def fin():
            self.exec_return_no_rows(session_factory, '''DROP FOREIGN TABLE {for_table_name}'''.format(for_table_name=self.for_table_name()))
        request.addfinalizer(fin)
        return None

    def test_foreign_table(self, session_factory, foreign_table):
        values = self.exec_return_values(session_factory, "SELECT * FROM information_schema.foreign_tables WHERE foreign_table_name='{0}'".format(self.for_table_name()))
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def exec_return_no_rows(self, session_factory, query):
        session = session_factory()
        sqlReturn = session.execute(query)

        assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)
        assert not sqlReturn.returns_rows, "NOT Expecting rows"

        session.commit()
        sqlReturn.close()

    # def exec_return_zero_rows(self, session_factory, query):
    #     values = self.exec_return_values(session_factory, query)
    #
    #     assert len(values) == 0, "Should be ZERO rows"

    def exec_return_empty(self, session_factory, query):
        values = self.exec_return_values(session_factory, query)

        assert len(values) == 1, "Expecting a single row"
        assert len(values[0].keys()) == 0, "Should not return any columns, found %s" % (keys)

    def exec_return_values(self, session_factory, query):
        session = session_factory()
        sqlReturn = session.execute(query)

        assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)
        assert sqlReturn.returns_rows, "Expecting rows"

        values = [dict(myval.items()) for myval in sqlReturn.fetchall()]

        session.commit()
        sqlReturn.close()

        return values

    def unordered_query(self, session_factory, query):
        query_ref = query.format(table_name=self.ref_table_name())
        query_for = query.format(table_name=self.for_table_name())

        return_ref = self.exec_return_values(session_factory, query_ref)
        return_for = self.exec_return_values(session_factory, query_for)

        assert len(return_ref) == len(return_for), "Expecting ref and for to have same number of returning rows"

        if not return_ref:
            return

        collection_ref = collections.Counter([tuple(myrow.items()) for myrow in return_ref])
        collection_for = collections.Counter([tuple(myrow.items()) for myrow in return_for])

        print('Checking match of ref:%s == for:%s' % (collection_ref, collection_for))
        assert collection_ref == collection_for, 'Expecting results from both queries to be identical apart from order'

    def ordered_query(self, session_factory, query):
        query_ref = query.format(table_name=self.ref_table_name())
        query_for = query.format(table_name=self.for_table_name())

        return_ref = self.exec_return_values(session_factory, query_ref)
        return_for = self.exec_return_values(session_factory, query_for)

        if not return_ref:
            return

        assert len(return_ref) == len(return_for), "Expecting ref and for to have same number of returning rows"

        for (row_ref, row_for) in zip(return_ref, return_for):
            print('Checking match of ref:%s == for:%s' % (row_ref, row_for))
            assert cmp(row_ref, row_for) == 0, 'Rows should match %s == %s' % (row_ref, row_for)
