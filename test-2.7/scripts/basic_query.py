import multicorn_test
import multicorn_test.mixed_data
from multicorn_test import db_engine, session_factory, session

import pytest


class TestBasicQuery(multicorn_test.MulticornBaseTest, multicorn_test.mixed_data.MixedData):

    @pytest.fixture(scope='function')
    def ref_table(self, request, session, db_engine):
        self.create_tables(db_engine)

        def fin():
            self.exec_no_return(session, '''DROP TABLE {0}'''.format(self.reference_table_name()))
        request.addfinalizer(fin)
        return None

    def test_ref_table(self, session, ref_table):
        (keys, values) = self.exec_return_value(session, 'SELECT * FROM {0}'.format(self.reference_table_name()))
        assert len(values) == 0, 'Expecting %s to be empty' % (self.reference_table_name())
        # assert 0, "Received keys=%s, values=%s" % (keys, values)

    @pytest.fixture(scope='function')
    def ref_table_loaded(self, request, session, ref_table):
        print('About to load data')
        self.load(session)
        session.commit()
        print('Should have loaded data')

        def fin():
            self.exec_no_return(session, '''DELETE FROM {0}'''.format(self.reference_table_name()))

        request.addfinalizer(fin)
        return None

    def test_ref_table_loaded(self, session, ref_table_loaded):
        (keys, values) = self.exec_return_value(session, 'SELECT * FROM {0}'.format(self.reference_table_name()))
        assert len(values) > 0, 'Expecting %s to be have some contents' % (self.reference_table_name())

    @pytest.fixture(scope='function')
    def helper_function(self, request, session, multicorn):
        self.exec_no_return(session, '''
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

        def fin():
            self.exec_no_return(session, '''DROP function create_foreign_server()''')
        request.addfinalizer(fin)
        return None

    def test_helper_function(self, session, helper_function):
        (keys, values) = self.exec_return_value(session, "SELECT * FROM information_schema.routines WHERE routine_type='FUNCTION' AND specific_schema='public' AND routine_name='create_foreign_server'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture
    def foreign_table(self, request, session, foreign_server, password):
        self.exec_no_return(session, '''
            create foreign table {0} (
              id integer,
              adate date,
              atimestamp timestamp,
              anumeric numeric,
              avarchar varchar
            ) server multicorn_srv options (
              tablename '{1}',
              password '{2}'
            )
            '''.format(self.foreign_table_name(), self.reference_table_name(), password))

        def fin():
            self.exec_no_return(session, '''DROP FOREIGN TABLE {0}'''.format(self.foreign_table_name()))
        request.addfinalizer(fin)
        return None

    def test_foreign_table(self, session, foreign_table, ref_table):
        (keys, values) = self.exec_return_value(session, "SELECT * FROM information_schema.foreign_tables WHERE foreign_table_name='{0}'".format(self.foreign_table_name()))
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    # @pytest.mark.xfail
    # @pytest.mark.run(order=110)
    # def test_copy_ref_to_for(self, session, ref_table_loaded):
    #     self.copy_ref_to_for(session)

    # @pytest.mark.skip(reason="Count is wrong when debugging")
    # @pytest.mark.run(order=15)
    # def test_select_into_query_ref(self, session):
    #     sqlReturn = session.execute('SELECT count(*) FROM query_ref')
    #     assert sqlReturn.returns_rows, "Should return some rows"
    #     numRows = sqlReturn.scalar()
    #     assert numRows == 0, "Should not be any data in the table"

    # --------------------------------------------------------------------------
    # Tests start here
    # --------------------------------------------------------------------------
    # @pytest.skip
    # @pytest.mark.parametrize("query", [
    #     '''SELECT * from {0} order by avarchar''',
    #     '''SELECT * from {0} order by avarchar desc''',
    #     '''SELECT * from {0} order by avarchar desc nulls first''',
    #     '''SELECT * from {0} order by avarchar desc nulls last''',
    #     '''SELECT * from {0} order by avarchar nulls first''',
    #     '''SELECT * from {0} order by avarchar nulls last''',
    #     '''SELECT * from {0} order by anumeric'''])
    # def test_ordered_query(self, session, query, ref_table_loaded):
    #     self.ordered_query(session, query)

    # @pytest.mark.parametrize("params_x", ['x_1', 'x_2'])
    # def test_check_params_single(self, params_x):
    #     print "testing %s", (params_x)
    #     assert 0, "Assuming we got params_x: %s" % (params_x)
    #
    # @pytest.mark.parametrize("params_y", ['y_1', 'y_2'])
    # @pytest.mark.parametrize("params_x", ['x_1', 'x_2'])
    # def test_check_params_double(self, params_x, params_y):
    #     print "testing x: %s", (params_x)
    #     print "testing y: %s", (params_y)
    #     assert 0, "Assuming we got params_x: %s, params_y:%s" % (params_x, params_y)
