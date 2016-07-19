import multicorn_test

import pytest
import os
import io


class TestInt(multicorn_test.MulticornBaseTest):
    @classmethod
    def table_columns(cls):
        return 'id INTEGER, value1 INTEGER, value2 INTEGER'

    @classmethod
    def sample_data(cls):
        return io.BytesIO('''id,value1,value2
1,1,0
2,2,0
3,3,0
4,4,0
5,3,0
6,2,0
7,1,0
8,-1,0
9,-2,0
10,-3,0
11,<None>,0
12,-2,0
''')

    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {0} order by RANDOM()'''),
        '''SELECT * from {0} order by value1''',
        '''SELECT * from {0} order by value1 desc''',
        '''SELECT * from {0} order by value1 desc nulls first''',
        '''SELECT * from {0} order by value1 desc nulls last''',
        '''SELECT * from {0} order by value1 nulls first''',
        '''SELECT * from {0} order by value1 nulls last'''])
    def test_ordered_query(self, session_factory, query, foreign_table, ref_table_populated):
        self.ordered_query(session_factory, query)

    @pytest.mark.parametrize("query", [
            pytest.mark.xfail(reason="deliberate random WHERE")('''SELECT * from {0} WHERE RANDOM() < 0.5'''),
            pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {0} ORDER BY RANDOM() LIMIT 5'''),
            '''SELECT * FROM {0}''',
            '''SELECT id,value1 FROM {0}''',
            '''SELECT * FROM {0} WHERE value1 IS NULL''',
            '''SELECT * FROM {0} WHERE value1 IS NOT NULL''',
            '''SELECT * from {0} where value1 > '0'::INTEGER''',
            '''SELECT * from {0} where value1 between 0 and 3 ''',
            '''SELECT * from {0} where value1 > 0''',
            '''SELECT * from {0} where value1 in (1,2)''',
            '''SELECT * from {0} where value1 not in (1, 2)''',
        ])
    def test_unordered_query(self, session_factory, query, foreign_table, ref_table_populated):
        self.unordered_query(session_factory, query)


class TestBasicQuery(multicorn_test.MulticornBaseTest):
    @classmethod
    def table_columns(cls):
        return 'id integer, adate date, atimestamp timestamp, anumeric numeric, avarchar varchar'

    @classmethod
    def sample_data(cls):
        output = io.BytesIO('''id,adate,atimestamp,anumeric,avarchar
1,'1980-01-01','1980-01-01  11:01:21.132912',3.4,'Test'
2,'1990-03-05','1998-03-02  10:40:18.321023',12.2,'Another Test'
3,'1972-01-02','1972-01-02  16:12:54.000000',4000,'another Test'
4,'1922-11-02','1962-01-02  23:12:54.000000',-3000,<None>
''')
        return output
        # return open(os.path.dirname(__file__)+'/mixed_data.csv', 'rb')

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
    # @pytest.mark.skip
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
