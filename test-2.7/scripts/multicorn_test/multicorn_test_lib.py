import pytest
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class MulticornBaseTest:

    @pytest.fixture(scope="module")
    def db_engine(self, request, username, password, db):
        print("Connecting to PG Engine")
        engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (username, password, db), echo=True)

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

    # SHould not create a session outside of the test as the session needs to close propoery inside the test to confirm complete.
    # @pytest.fixture(scope="function")
    # def session(self, request, session_factory):
    #     print("Creating Session")
    #     sess = session_factory()
    #
    #     def fin():
    #         sess.commit()
    #         sess.close()
    #         print("Commited Session")
    #
    #     request.addfinalizer(fin)
    #     return sess

    def exec_sql(self, session_factory, query):
        session = session_factory()
        sqlReturn = session.execute(query)
        assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)
        session.commit()
        session.close()
        return sqlReturn

    def exec_no_return(self, session_factory, query):
        returnVal = self.exec_sql(session_factory, query)
        assert not returnVal.returns_rows, "Not expecting any rows"

    def exec_return_empty(self, session_factory, query):
        returnVal = self.exec_sql(session_factory, query)
        assert returnVal.returns_rows, "Expecting rows"
        assert returnVal.rowcount == 1, "Expecting a single row"
        assert len(returnVal.keys()) == 0, "Should not return any columns, found %s" % (returnVal.keys())

    def exec_return_value(self, session_factory, query):
        returnVal = self.exec_sql(session_factory, query)
        assert returnVal.returns_rows, "Expecting rows"
        return (returnVal.keys(), returnVal.fetchall())

    @pytest.fixture(scope='function')
    def multicorn(self, request, session_factory):
        self.exec_no_return(session_factory, '''CREATE EXTENSION multicorn''')

        def fin():
            self.exec_no_return(session_factory, '''DROP EXTENSION multicorn''')

        request.addfinalizer(fin)
        return None

    def test_multicorn(self, session_factory, multicorn):
        (keys, values) = self.exec_return_value(session_factory, "SELECT * FROM pg_catalog.pg_extension WHERE extname='multicorn'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture(scope='function')
    def foreign_server(self, request, session_factory, helper_function):
        (keys, values) = self.exec_return_value(session_factory, '''SELECT create_foreign_server()''')
        assert 1, "Do not care about return keys or values"

        def fin():
            self.exec_no_return(session_factory, '''DROP SERVER multicorn_srv''')
        request.addfinalizer(fin)
        return None

    def test_foreign_server(self, session_factory, foreign_server):
        (keys, values) = self.exec_return_value(session_factory, "SELECT * FROM information_schema.foreign_servers WHERE foreign_server_name='multicorn_srv'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    def unordered_query(self, session_factory, query):
        assert 0, 'this does not test properly yet'

    def ordered_query(self, session_factory, query):
        query_ref = query.format(self.ref_table_name())
        query_for = query.format(self.for_table_name())

        return_ref = self.exec_sql(session_factory, query_ref)
        return_for = self.exec_sql(session_factory, query_for)

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
    def test_connection(self, session_factory):
        self.exec_return_empty(session_factory, 'SELECT')
