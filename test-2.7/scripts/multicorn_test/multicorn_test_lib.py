import pytest
from sqlalchemy.sql import text


class MulticornBaseTest:
    @pytest.fixture(scope='function')
    def multicorn(self, request, session):
        self.exec_no_return(session, '''CREATE EXTENSION multicorn''')

        def fin():
            self.exec_no_return(session, '''DROP EXTENSION multicorn''')
        request.addfinalizer(fin)
        return None

    def test_multicorn(self, session, multicorn):
        (keys, values) = self.exec_return_value(session, "SELECT * FROM pg_catalog.pg_extension WHERE extname='multicorn'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture(scope='function')
    def foreign_server(self, request, session, helper_function):
        (keys, values) = self.exec_return_value(session, '''SELECT create_foreign_server()''')
        assert 1, "Do not care about return keys or values"

        def fin():
            self.exec_no_return(session, '''DROP SERVER multicorn_srv''')
        request.addfinalizer(fin)
        return None

    def test_foreign_server(self, session, foreign_server):
        (keys, values) = self.exec_return_value(session, "SELECT * FROM information_schema.foreign_servers WHERE foreign_server_name='multicorn_srv'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    def exec_no_return(self, session, query):
        returnVal = session.execute(query)
        assert not returnVal.returns_rows, "Not expecting any rows"

    def exec_return_empty(self, session, query):
        returnVal = session.execute(query)
        assert returnVal.returns_rows, "Expecting rows"
        assert returnVal.rowcount == 1, "Expecting a single row"
        assert len(returnVal.keys()) == 0, "Should not return any columns, found %s" % (returnVal.keys())

    def exec_return_value(self, session, query):
        returnVal = session.execute(query)
        assert returnVal.returns_rows, "Expecting rows"
        return (returnVal.keys(), returnVal.fetchall())

    def copy_ref_to_for(self, session):
        self.exec_no_return(session, '''INSERT INTO {1} SELECT * from {0}'''.format(self.reference_table_name(), self.foreign_table_name()))

    def ordered_query(self, session, query):
        query_ref = query.format(self.reference_table_name())
        query_for = query.format(self.foreign_table_name())
        print("RUNNING ref:%s and for:%s" % (query_ref, query_for))

        return_ref = session.execute(query_ref)
        return_for = session.execute(query_for)
        print("ref:%s and for:%s" % (return_ref, return_for))
        print("looking at ref")

        assert return_ref.returns_rows == return_for.returns_rows, "Expecting ref and for to have matching returns_rows"

        if not return_ref.returns_rows:
            return

        assert return_ref.rowcount == return_for.rowcount, "Expecting ref and for to have same number of returning rows"

        result_ref = return_ref.fetchall()
        result_for = return_for.fetchall()

        for (row_ref, row_for) in zip(result_ref, result_for):
            assert row_ref == row_for, 'Rows should match %s == %s' % (row_ref, row_for)

    # --------------------------------------------------------------------------
    # Default tests always run
    # --------------------------------------------------------------------------
    @pytest.mark.run(order=20)
    def test_connection(self, session):
        self.exec_return_empty(session, 'SELECT')
