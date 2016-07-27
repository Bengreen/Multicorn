from multicorn_test import MulticornBaseTest
import pytest
import os
import io


class TestFDW(MulticornBaseTest):
    """
        This class runs our suite of SQL tests against a foreign data wrapper.
        The test sets up two tables:
         - a reference table in PostgreSQL
         - a foreign table in the external database
        Each SQL test is run against both, and the results compared

        By default the foreign table is setup to point at the reference table (via the psql FDW), so all tests should pass
        To test a new foreign data wrapper, inherit this class and override:
         - for_table_populated
         - fdw
         - fdw_options
    """

    #--------------------------------------------------------------------------
    # Test data setup
    # --------------------------------------------------------------------------

    # TODO the test data we use needs more discussion/thought...
    @pytest.fixture(scope="class")
    def table_columns(self, request):
        return 'int1 INTEGER, int2 INTEGER, real1 REAL, real2 REAL, date1 DATE, date2 DATE, timestamp1 TIMESTAMP, timestamp2 TIMESTAMP, string TEXT'

    # TODO this should probably be randomly generated, and definitely should be much larger
    @classmethod
    def sample_data(cls):
        return '''int1,int2,real1,real2,date1,date2,timestamp1,timestamp2,string
14,63,-8.37,97.10,2218-02-26,2050-11-18,2075-07-21 22:20:56,2044-09-10 22:47:56,flb
-95,-13,-35.82,35.98,2193-09-18,2100-09-09,2141-09-05 04:22:18,2219-07-11 02:04:03,hag
-92,-67,-49.05,-43.24,2137-10-18,2204-10-20,2123-08-27 09:22:46,2041-04-03 17:52:27,wbk
-51,62,-68.16,73.70,2005-02-25,2040-01-30,2206-06-14 12:17:12,2164-05-26 11:05:36,wmw
23,-90,-72.61,27.02,2007-06-17,2190-10-19,1995-10-22 02:04:46,2160-01-05 15:46:46,rin
<None>,-74,-87.11,-10.60,2230-07-22,2157-08-02,1991-11-11 04:41:25,2145-06-03 02:50:33,gfj
37,-18,-5.96,-20.12,2014-04-11,2209-11-30,1977-05-03 11:37:58,2044-05-29 05:32:52,xok
-76,58,-85.43,34.83,2228-05-24,2208-06-11,2037-10-29 10:49:42,2228-03-04 15:30:33,wlf
-67,-87,-83.23,71.59,2031-07-07,2192-10-01,2005-11-17 00:30:29,1983-10-03 08:45:50,lzg
75,<None>,92.83,-7.07,2016-12-03,2045-01-18,2085-06-29 17:51:53,2195-08-05 02:15:00,kgy
26,-14,-0.07,54.62,2183-03-09,2001-01-01,2141-09-02 20:58:52,2098-06-21 06:45:00,jja
-62,-90,-52.98,84.41,2118-09-04,1983-11-19,2060-07-30 17:00:35,2059-11-22 08:43:47,mup
-8,-48,19.06,<None>,1991-04-27,2003-10-27,2167-10-16 18:34:11,<None>,rhq
83,90,-63.16,34.44,2054-08-07,2124-01-23,2023-08-12 23:52:50,2109-06-29 06:03:20,uuh
86,68,-24.80,57.00,2031-02-23,<None>,2030-06-05 16:03:21,2103-12-14 14:33:01,rjd
-73,38,2.84,-89.88,2126-08-24,2123-12-03,2048-02-03 13:14:07,2223-04-01 03:25:42,vht
-89,31,-11.42,64.13,2207-08-09,1973-09-16,2122-02-28 18:31:33,1972-07-20 00:05:28,mnh
-80,48,-56.49,-19.11,2111-10-14,2205-09-06,2126-05-18 00:26:52,2237-07-12 05:22:13,<None>
-23,65,84.00,-5.08,2121-10-15,2129-08-30,2005-11-01 11:18:51,1995-02-23 22:11:44,iom
40,34,-70.76,-61.05,2103-05-20,1984-02-15,2013-09-29 12:36:21,2075-11-04 12:05:36,hpb
'''

    @classmethod
    def sample_io(cls):
        return io.BytesIO(cls.sample_data())


    # --------------------------------------------------------------------------
    # SQL Test Suite
    # --------------------------------------------------------------------------

    # ------------------------ #
    # --- Failing queries ---- #
    # ------------------------ #
    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {table_name} order by RANDOM()''')
        ])
    def test_failing_ordered(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
        #import pdb; pdb.set_trace()
        self.ordered_query(connection, query)

    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate random WHERE")('''SELECT * from {table_name} WHERE RANDOM() < 0.5'''),
        pytest.mark.xfail(reason="deliberate random ORDER and LIMIT")('''SELECT * from {table_name} ORDER BY RANDOM() LIMIT 5''')
        ])
    def test_failing_unordered(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query)


    # ------------------------- #
    # ---- Very basic SQL  ---- #
    # ------------------------- #
    @pytest.mark.parametrize("query", [
        '''SELECT * from {table_name}''',
        '''SELECT 1, * from {table_name} ''',
        '''SELECT 'abs' AS text_col, * FROM {table_name}''',
        '''SELECT COUNT(*) FROM {table_name}'''
        ])
    def test_basic(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query)


    # ------------------------ #
    # --- Basic SELECT SQL --- #
    # ------------------------ #
    @pytest.mark.parametrize("column", [
        'int1',
        'int2',
        'real1',
        'real2',
        'date1',
        'date2',
        'timestamp1',
        'timestamp2',
        'string'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column} FROM {table_name}''',
        '''SELECT {column} AS alias FROM {table_name}''',
        '''SELECT DISTINCT {column} FROM {table_name}'''
        ])
    def test_select(self, column, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column=column))

    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2',
        'date1',
        'date2',
        'timestamp1',
        'timestamp2',
        'string'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'int2',
        'real1',
        'real2',
        'date1',
        'date2',
        'timestamp1',
        'timestamp2',
        'string'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1}, {column2} FROM {table_name}''',
        '''SELECT DISTINCT {column1}, {column2} FROM {table_name}'''
        ])
    def test_multiple_select(self, column1, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2))


    # -------------------------------- #
    # ---- Test SELECT arithmetic ---- #
    # -------------------------------- #
    # Only test arithmetic on the numerical columns (TODO maybe expand later to other types)
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("operator", [
        '+',
        '-',
        '*',
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'int2',
        'real1',
        'real2',
        '2',
        '1',
        '0',
        '-1',
        '-2',
        '2.34',
        '-2.34'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1} {operator} {column2} FROM {table_name}''',
        ])
    def test_select_arithmetic1(self, column1, column2, operator, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2, operator=operator))

    # Test divide against specified constants (to avoid div by zero issues)
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("operator", [
        '/'
        ])
    @pytest.mark.parametrize("column2", [
        '2',
        '1',
        '-1',
        '-2',
        '2.34',
        '23.4',
        '-2.34'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1} {operator} {column2} FROM {table_name}''',
        ])
    def test_select_arithmetic2(self, column1, column2, operator, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2, operator=operator))

    # Test exponentiation with only positive ints to avoid complex numbers and that whole mess
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("column2", [
        '0',
        '1',
        '2',
        '10'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1} ^ {column2} FROM {table_name}''',
        ])
    def test_select_arithmetic3(self, column1, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2))


    # ------------------------------- #
    # ---- Test SELECT functions ---- #
    # ------------------------------- #
    # TODO should probably add some other funcs in here e.g. round, trunc, log etc.
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("function", [
        'ABS',
        'FLOOR',
        'CEILING',
        'EXP',
        'SIGN'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {function}({column1}) FROM {table_name}''',
        '''SELECT DISTINCT {function}({column1}) FROM {table_name}'''
        ])
    def test_select_functions(self, column1, function, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function))


    # ------------------------------ #
    # ---- Test WHERE operators ---- #
    # ------------------------------ #

    # First for numerical types
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("operator", [
        '<',
        '>',
        '<=',
        '>=',
        '=',
        '!=',
        '<>'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'int2',
        'real1',
        'real2',
        '1',
        '0',
        '-1',
        '1.23',
        '-1.23'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} {operator} {column2}'''
        ])
    def test_where_operators_numbers(self, column1, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, operator=operator, column2=column2))

    # Then for time types
    @pytest.mark.parametrize("column1", [
        'date1',
        'date2',
        'timestamp1',
        'timestamp2'
        ])
    @pytest.mark.parametrize("operator", [
        '<',
        '>',
        '<=',
        '>=',
        '=',
        '!=',
        '<>'
        ])
    @pytest.mark.parametrize("column2", [
        'date1',
        'date2',
        'timestamp1',
        'timestamp2',
        "'2016-07-27'",
        "'1986-12-19 12:00:00'",
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} {operator} {column2}'''
        ])
    def test_where_operators_times(self, column1, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, operator=operator, column2=column2))


    # -------------------------------------- #
    # ---- Test WHERE logical operators ---- #
    # -------------------------------------- #

    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("operator", [
        'AND',
        'OR'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} > 0 {operator} {column2} > 0'''
        ])
    def test_where_logical_operators(self, column1, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, operator=operator, column2=column2))


    # ------------------------------ #
    # ---- Test WHERE functions ---- #
    # ------------------------------ #

    # Test out a few functions in the where clause (only on numerical types)
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("function", [
        'ABS',
        'FLOOR',
        'CEILING',
        'EXP',
        'SIGN'
        ])
    @pytest.mark.parametrize("operator", [
        '<',
        '>',
        '<=',
        '>=',
        '=',
        '!=',
        '<>'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {function}({column1}) {operator} {column2}'''
        ])
    def test_where_logical_operators(self, column1, function, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, operator=operator, column2=column2))


    # -------------------------------------------- #
    # ---- Test text specific WHERE functions ---- #
    # -------------------------------------------- #

    # TODO this needs more work/thought, but it's a starting point anyway...
    # TODO probably should also be expanded to include some SIMILAR TO (proper regex) tests, but that's gonna be all kinds of problems
    @pytest.mark.parametrize("regex", [
        "'%%'",
        "'a%%'",
        "'z%%'",
        "'%%a'",
        "'%%z'",
        "'a_'",
        "'z_'",
        "'_a'",
        "'_z'",
        "'_a%%'",
        "'_z%%'",
        "'%%a_'",
        "'%%z_'",
        "'_a_'",
        "'_z_'",
        "'_a_'",
        "'_z_'"
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE string LIKE {regex}''',
        '''SELECT * FROM {table_name} WHERE string ILIKE {regex}'''
        ])
    def test_string_like(self, regex, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', regex=regex))


#    @pytest.mark.parametrize("query", [
#        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {table_name} order by RANDOM()'''),
#        '''SELECT * from {table_name} order by value1''',
#        '''SELECT * from {table_name} order by value1 desc''',
#        '''SELECT * from {table_name} order by value1 desc nulls first''',
#        '''SELECT * from {table_name} order by value1 desc nulls last''',
#        '''SELECT * from {table_name} order by value1 nulls first''',
#        '''SELECT * from {table_name} order by value1 nulls last''',
#        ])
#    def test_ordered_query(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
#        self.ordered_query(connection, query)
#
#    # Build out the operators and columns for the table
#    @pytest.mark.parametrize('operator', [
#        '>',
#        '<',
#        '=',
#        '!=',
#        '<>',
#        '>=',
#        '<=',
#        ])
#    @pytest.mark.parametrize('left_value', [
#        'value1',
#        'value2',
#        ])
#    @pytest.mark.parametrize('right_value', [
#        'value1',
#        'value2',
#        ])
#    @pytest.mark.parametrize('query', [
#        '''SELECT * from {table_name} where {left_value} {operator} {right_value}''',
#        ])
#    def test_operators(self, left_value, operator, right_value, connection, query, foreign_table, ref_table_populated, for_table_populated):
#        self.unordered_query(connection, query.format(table_name='{table_name}', left_value=left_value, operator=operator, right_value=right_value))
#
#    # Build out example queries
#    @pytest.mark.parametrize("query", [
#        # Force fail tests to confirm that the test framework is operating correctly
#        pytest.mark.xfail(reason="deliberate random WHERE")('''SELECT * from {table_name} WHERE RANDOM() < 0.5'''),
#        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {table_name} ORDER BY RANDOM() LIMIT 5'''),
#
#        # Test for quotes in query
#        '''SELECT * FROM {table_name} WHERE value1 = value1 ''',
#
#        # Simple query tests to confirm basic query operations
#        '''SELECT * FROM {table_name}''',
#        '''SELECT id,value1 FROM {table_name}''',
#        '''SELECT count(*) FROM {table_name}''',
#        # Test for presence of NULL
#        '''SELECT * FROM {table_name} WHERE value1 IS NULL''',
#        '''SELECT * FROM {table_name} WHERE value1 IS NOT NULL''',
#        # Test comparison with integers (explicit conversion)
#        '''SELECT * from {table_name} where value1 > '1'::INTEGER''',
#        '''SELECT * from {table_name} where value1 < '1'::INTEGER''',
#        '''SELECT * from {table_name} where value1 = '1'::INTEGER''',
#        '''SELECT * from {table_name} where value1 != '1'::INTEGER''',
#        '''SELECT * from {table_name} where value1 >= '1'::INTEGER''',
#        '''SELECT * from {table_name} where value1 >= '1'::INTEGER''',
#        # Test comparison with integers (implicit conversion)
#        '''SELECT * from {table_name} where value1 > 1''',
#        '''SELECT * from {table_name} where value1 < 1''',
#        '''SELECT * from {table_name} where value1 = 1''',
#        '''SELECT * from {table_name} where value1 != 1''',
#        '''SELECT * from {table_name} where value1 >= 1''',
#        '''SELECT * from {table_name} where value1 <= 1''',
#        # Test comparison with float (implicit conversion)
#        '''SELECT * from {table_name} where value1 > 1.0''',
#        '''SELECT * from {table_name} where value1 < 1.0''',
#        '''SELECT * from {table_name} where value1 = 1.0''',
#        '''SELECT * from {table_name} where value1 != 1.0''',
#        '''SELECT * from {table_name} where value1 >= 1.0''',
#        '''SELECT * from {table_name} where value1 <= 1.0''',
#        # Test comparison with integer (from value2)
#        '''SELECT * from {table_name} where value1 > value2''',
#        '''SELECT * from {table_name} where value1 < value2''',
#        '''SELECT * from {table_name} where value1 = value2''',
#        '''SELECT * from {table_name} where value1 != value2''',
#        '''SELECT * from {table_name} where value1 >= value2''',
#        '''SELECT * from {table_name} where value1 <= value2''',
#        # Test between operator
#        '''SELECT * from {table_name} where value1 between 1 and 2''',
#        # Test in, not in operator
#        '''SELECT * from {table_name} where value1 in (1,2)''',
#        '''SELECT * from {table_name} where value1 not in (1, 2)''',
#        # Test Group operator
#        '''SELECT count(*) FROM {table_name} GROUP BY value1''',
#        ])
#    def test_unordered_query(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
#        self.unordered_query(connection, query)


#class TestOriginalQueryFromMulticorn(MulticornBaseTest):
#
#    @pytest.fixture(scope="class")
#    def table_columns(self, request):
#        return 'id integer, adate date, atimestamp timestamp, anumeric numeric, avarchar varchar'

#    @classmethod
#    def sample_io(cls):
#        output = io.BytesIO('''id,adate,atimestamp,anumeric,avarchar
#1,'1980-01-01','1980-01-01  11:01:21.132912',3.4,'Test'
#2,'1990-03-05','1998-03-02  10:40:18.321023',12.2,'Another Test'
#3,'1972-01-02','1972-01-02  16:12:54.000000',4000,'another Test'
#4,'1922-11-02','1962-01-02  23:12:54.000000',-3000,<None>
#''')
#        return output
#        # return open(os.path.dirname(__file__)+'/mixed_data.csv', 'rb')
#
#    # --------------------------------------------------------------------------
#    # Tests start here
#    # --------------------------------------------------------------------------
#    # @pytest.mark.skip
#    @pytest.mark.parametrize("query", [
#        '''SELECT * from {table_name} order by avarchar''',
#        '''SELECT * from {table_name} order by avarchar desc''',
#        '''SELECT * from {table_name} order by avarchar desc nulls first''',
#        '''SELECT * from {table_name} order by avarchar desc nulls last''',
#        '''SELECT * from {table_name} order by avarchar nulls first''',
#        '''SELECT * from {table_name} order by avarchar nulls last''',
#        '''SELECT * from {table_name} order by anumeric'''])
#    def test_ordered_query(self, connection, query, foreign_table, ref_table_populated):
#        self.ordered_query(connection, query)
#
#    # @pytest.mark.xfail
#    # @pytest.mark.skip
#    @pytest.mark.parametrize("query", [
#        '''SELECT * FROM {table_name}''',
#        '''SELECT id,atimestamp FROM {table_name}''',
#        '''SELECT * FROM {table_name} WHERE avarchar IS NULL''',
#        '''SELECT * FROM {table_name} WHERE avarchar IS NOT NULL''',
#        '''SELECT * from {table_name} where adate > '1970-01-02'::date''',
#        '''SELECT * from {table_name} where adate between '1970-01-01' and '1980-01-01' ''',
#        '''SELECT * from {table_name} where anumeric > 0''',
#        '''SELECT * from {table_name} where avarchar not like '%%test' ''',
#        '''SELECT * from {table_name} where avarchar like 'Another%%' ''',
#        '''SELECT * from {table_name} where avarchar ilike 'Another%%' ''',
#        '''SELECT * from {table_name} where avarchar not ilike 'Another%%' ''',
#        '''SELECT * from {table_name} where id in (1,2)''',
#        '''SELECT * from {table_name} where id not in (1, 2)''',
#        ])
#    def test_unordered_query(self, connection, query, foreign_table, ref_table_populated):
#        self.unordered_query(connection, query)
