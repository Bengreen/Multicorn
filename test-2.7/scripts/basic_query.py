import multicorn_test
import multicorn_test.mixed_data
# from multicorn_test import db_engine, session_factory, session

import pytest


class TestBasicQuery(multicorn_test.MulticornBaseTest, multicorn_test.mixed_data.MixedData):

    # @pytest.fixture(scope='function')
    # def ref_table(self, request, session_factory, db_engine):
    #     self.create_tables(session_factory)
    #
    #     def fin():
    #         self.exec_no_return(session_factory, '''DROP TABLE {0}'''.format(self.ref_table_name()))
    #     request.addfinalizer(fin)
    #     return None



    # @pytest.fixture(scope='function')
    # def ref_table_loaded(self, request, session_factory, ref_table):
    #     self.load(session_factory)
    #
    #     def fin():
    #         self.exec_no_return(session_factory, '''DELETE FROM {0}'''.format(self.ref_table_name()))
    #
    #     request.addfinalizer(fin)
    #     return None
    #
    # def test_ref_table_loaded(self, session_factory, ref_table_loaded):
    #     (keys, values) = self.exec_return_value(session_factory, 'SELECT * FROM {0}'.format(self.ref_table_name()))
    #     assert len(values) > 0, 'Expecting %s to be have some contents' % (self.ref_table_name())

    @pytest.fixture(scope='function')
    def helper_function(self, request, session_factory, multicorn):
        self.exec_no_return(session_factory, '''
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
            self.exec_no_return(session_factory, '''DROP function create_foreign_server()''')
        request.addfinalizer(fin)
        return None

    def test_helper_function(self, session_factory, helper_function):
        (keys, values) = self.exec_return_value(session_factory, "SELECT * FROM information_schema.routines WHERE routine_type='FUNCTION' AND specific_schema='public' AND routine_name='create_foreign_server'")
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    @pytest.fixture
    def foreign_table(self, request, session_factory, foreign_server, password):
        self.exec_no_return(session_factory, '''
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
            '''.format(self.for_table_name(), self.ref_table_name(), password))

        def fin():
            self.exec_no_return(session_factory, '''DROP FOREIGN TABLE {0}'''.format(self.for_table_name()))
        request.addfinalizer(fin)
        return None

    def test_foreign_table(self, session_factory, foreign_table, ref_table):
        (keys, values) = self.exec_return_value(session_factory, "SELECT * FROM information_schema.foreign_tables WHERE foreign_table_name='{0}'".format(self.for_table_name()))
        assert len(values) == 1, 'Expecting one record got %s' % (values)

    # --------------------------------------------------------------------------
    # Tests start here
    # --------------------------------------------------------------------------
    # @pytest.mark.skip
    @pytest.mark.parametrize("query", [
        '''SELECT * from {0} order by avarchar''',
        '''SELECT * from {0} order by avarchar desc''',
        '''SELECT * from {0} order by avarchar desc nulls first''',
        '''SELECT * from {0} order by avarchar desc nulls last''',
        '''SELECT * from {0} order by avarchar nulls first''',
        '''SELECT * from {0} order by avarchar nulls last''',
        '''SELECT * from {0} order by anumeric'''])
    def test_ordered_query(self, session_factory, query, foreign_table, ref_table_populated):
        self.ordered_query(session_factory, query)

    # @pytest.mark.xfail
    @pytest.mark.parametrize("query", [
            '''SELECT * FROM {0}''',
            '''SELECT id,atimestamp FROM {0}''',
            '''SELECT * FROM {0} WHERE avarchar IS NULL''',
            '''SELECT * FROM {0} WHERE avarchar IS NOT NULL''',
            '''SELECT * from {0} where adate > '1970-01-02'::date''',
            '''SELECT * from {0} where adate between '1970-01-01' and '1980-01-01' ''',
            '''SELECT * from {0} where anumeric > 0''',
            '''SELECT * from {0} where avarchar not like '%%test' ''',
            '''SELECT * from {0} where avarchar like 'Another%%' ''',
            '''SELECT * from {0} where avarchar ilike 'Another%%' ''',
            '''SELECT * from {0} where avarchar not ilike 'Another%%' ''',
            '''SELECT * from {0} where id in (1,2)''',
            '''SELECT * from {0} where id not in (1, 2)''',
        ])
    def test_unordered_query(self, session_factory, query, foreign_table, ref_table_populated):
        self.unordered_query(session_factory, query)

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
