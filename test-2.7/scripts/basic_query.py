import multicorn_test

import pytest
import os
import io


# aeHiveUdv.aeHiveUdv

class TestVarchar(multicorn_test.MulticornBaseTest):

    @pytest.fixture(scope="class")
    def table_columns(self, request):
        return 'id INTEGER, value1 VARCHAR, value2 VARCHAR'

    @classmethod
    def sample_data(cls):
        return '''id,value1,value2
1,'a','a'
2,'b','c'
3,'c','c'
'''

    @classmethod
    def sample_io(cls):
        return io.BytesIO(cls.sample_data())

    @pytest.mark.parametrize("query", [
        # Force fail tests to confirm that the test framework is operating correctly
        '''SELECT * FROM {table_name} WHERE value1 = value2''',
        '''SELECT * FROM {table_name} WHERE value1 = 'value2' ''',
        ])
    def test_unordered_query(self, session_factory, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(session_factory, query)


class TestInt(multicorn_test.MulticornBaseTest):

    # @pytest.fixture(scope="class")
    # def data_type_value1(self, request):
    #     return xxx,yyy

    # @pytest.mark.parametrize("value1, value2, value3", [
    #     ('INTEGER', 'INTEGER', 'INTEGER'),
    #     ('INTEGER', 'INTEGER', 'REAL'),
    #     ])
    @pytest.fixture(scope="class", params=[
        ('INTEGER', 'INTEGER', 'INTEGER'),
        ('INTEGER', 'INTEGER', 'REAL'),
        ])
    def table_columns_types(self, request):
        return request.param

    @pytest.fixture(scope="class")
    def table_columns(self, request, table_columns_types):
        (value1, value2, value3) = table_columns_types
        return 'id INTEGER, value1 {value1}, value2 {value2}, value3 {value3}'.format(value1=value1, value2=value2, value3=value3)

    @classmethod
    def sample_data(cls):
        return '''id,value1,value2
1,1,1
2,2,1
3,3,1
4,4,1
5,3,1
6,2,1
7,1,1
8,-1,1
9,-2,1
10,-3,1
11,<None>,1
12,-2,1
'''

    @classmethod
    def sample_io(cls):
        return io.BytesIO(cls.sample_data())

    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {table_name} order by RANDOM()'''),
        '''SELECT * from {table_name} order by value1''',
        '''SELECT * from {table_name} order by value1 desc''',
        '''SELECT * from {table_name} order by value1 desc nulls first''',
        '''SELECT * from {table_name} order by value1 desc nulls last''',
        '''SELECT * from {table_name} order by value1 nulls first''',
        '''SELECT * from {table_name} order by value1 nulls last''',
        ])
    def test_ordered_query(self, session_factory, query, foreign_table, ref_table_populated, for_table_populated):
        self.ordered_query(session_factory, query)

    # Build out the operators and columns for the table
    @pytest.mark.parametrize('operator', [
        '>',
        '<',
        '=',
        '!=',
        '<>',
        '>=',
        '<=',
        ])
    @pytest.mark.parametrize('left_value', [
        'value1',
        'value2',
        ])
    @pytest.mark.parametrize('right_value', [
        'value1',
        'value2',
        ])
    @pytest.mark.parametrize('query', [
        '''SELECT * from {table_name} where {left_value} {operator} {right_value}''',
        ])
    def test_operators(self, left_value, operator, right_value, session_factory, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(session_factory, query.format(table_name='{table_name}', left_value=left_value, operator=operator, right_value=right_value))

    # Build out example queries
    @pytest.mark.parametrize("query", [
        # Force fail tests to confirm that the test framework is operating correctly
        pytest.mark.xfail(reason="deliberate random WHERE")('''SELECT * from {table_name} WHERE RANDOM() < 0.5'''),
        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {table_name} ORDER BY RANDOM() LIMIT 5'''),

        # Test for quotes in query
        '''SELECT * FROM {table_name} WHERE value1 = value1 ''',

        # Simple query tests to confirm basic query operations
        '''SELECT * FROM {table_name}''',
        '''SELECT id,value1 FROM {table_name}''',
        '''SELECT count(*) FROM {table_name}''',
        # Test for presence of NULL
        '''SELECT * FROM {table_name} WHERE value1 IS NULL''',
        '''SELECT * FROM {table_name} WHERE value1 IS NOT NULL''',
        # Test comparison with integers (explicit conversion)
        '''SELECT * from {table_name} where value1 > '1'::INTEGER''',
        '''SELECT * from {table_name} where value1 < '1'::INTEGER''',
        '''SELECT * from {table_name} where value1 = '1'::INTEGER''',
        '''SELECT * from {table_name} where value1 != '1'::INTEGER''',
        '''SELECT * from {table_name} where value1 >= '1'::INTEGER''',
        '''SELECT * from {table_name} where value1 >= '1'::INTEGER''',
        # Test comparison with integers (implicit conversion)
        '''SELECT * from {table_name} where value1 > 1''',
        '''SELECT * from {table_name} where value1 < 1''',
        '''SELECT * from {table_name} where value1 = 1''',
        '''SELECT * from {table_name} where value1 != 1''',
        '''SELECT * from {table_name} where value1 >= 1''',
        '''SELECT * from {table_name} where value1 <= 1''',
        # Test comparison with float (implicit conversion)
        '''SELECT * from {table_name} where value1 > 1.0''',
        '''SELECT * from {table_name} where value1 < 1.0''',
        '''SELECT * from {table_name} where value1 = 1.0''',
        '''SELECT * from {table_name} where value1 != 1.0''',
        '''SELECT * from {table_name} where value1 >= 1.0''',
        '''SELECT * from {table_name} where value1 <= 1.0''',
        # Test comparison with integer (from value2)
        '''SELECT * from {table_name} where value1 > value2''',
        '''SELECT * from {table_name} where value1 < value2''',
        '''SELECT * from {table_name} where value1 = value2''',
        '''SELECT * from {table_name} where value1 != value2''',
        '''SELECT * from {table_name} where value1 >= value2''',
        '''SELECT * from {table_name} where value1 <= value2''',
        # Test between operator
        '''SELECT * from {table_name} where value1 between 1 and 2''',
        # Test in, not in operator
        '''SELECT * from {table_name} where value1 in (1,2)''',
        '''SELECT * from {table_name} where value1 not in (1, 2)''',
        # Test Group operator
        '''SELECT count(*) FROM {table_name} GROUP BY value1''',
        ])
    def test_unordered_query(self, session_factory, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(session_factory, query)


class TestOriginalQueryFromMulticorn(multicorn_test.MulticornBaseTest):

    @pytest.fixture(scope="class")
    def table_columns(self, request):
        return 'id integer, adate date, atimestamp timestamp, anumeric numeric, avarchar varchar'

    @classmethod
    def sample_io(cls):
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
        '''SELECT * from {table_name} order by avarchar''',
        '''SELECT * from {table_name} order by avarchar desc''',
        '''SELECT * from {table_name} order by avarchar desc nulls first''',
        '''SELECT * from {table_name} order by avarchar desc nulls last''',
        '''SELECT * from {table_name} order by avarchar nulls first''',
        '''SELECT * from {table_name} order by avarchar nulls last''',
        '''SELECT * from {table_name} order by anumeric'''])
    def test_ordered_query(self, session_factory, query, foreign_table, ref_table_populated):
        self.ordered_query(session_factory, query)

    # @pytest.mark.xfail
    # @pytest.mark.skip
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name}''',
        '''SELECT id,atimestamp FROM {table_name}''',
        '''SELECT * FROM {table_name} WHERE avarchar IS NULL''',
        '''SELECT * FROM {table_name} WHERE avarchar IS NOT NULL''',
        '''SELECT * from {table_name} where adate > '1970-01-02'::date''',
        '''SELECT * from {table_name} where adate between '1970-01-01' and '1980-01-01' ''',
        '''SELECT * from {table_name} where anumeric > 0''',
        '''SELECT * from {table_name} where avarchar not like '%%test' ''',
        '''SELECT * from {table_name} where avarchar like 'Another%%' ''',
        '''SELECT * from {table_name} where avarchar ilike 'Another%%' ''',
        '''SELECT * from {table_name} where avarchar not ilike 'Another%%' ''',
        '''SELECT * from {table_name} where id in (1,2)''',
        '''SELECT * from {table_name} where id not in (1, 2)''',
        ])
    def test_unordered_query(self, session_factory, query, foreign_table, ref_table_populated):
        self.unordered_query(session_factory, query)
