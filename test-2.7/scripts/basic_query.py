from multicorn_test import MulticornBaseTest
import pytest
import os
import io
import random
import string


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
        '''SELECT * FROM {table_name} WHERE {column1} {operator} {column2}''',
        '''SELECT * FROM {table_name} WHERE NOT {column1} {operator} {column2}'''
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
        '''SELECT * FROM {table_name} WHERE {column1} {operator} {column2}''',
        '''SELECT * FROM {table_name} WHERE NOT {column1} {operator} {column2}'''
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
        '''SELECT * FROM {table_name} WHERE {column1} > 0 {operator} {column2} > 0''',
        '''SELECT * FROM {table_name} WHERE NOT {column1} > 0 {operator} {column2} > 0'''
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

    # Test out the IN operator
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE string IN {string_list}''',
        '''SELECT * FROM {table_name} WHERE NOT string IN {string_list}'''
        ])
    def test_string_in(self, query, connection, foreign_table, ref_table_populated, for_table_populated):
        # TODO this list should be changed to match what's in data (should return some, not all or none)
        string_list = "('%s')" % "','".join([''.join(random.choice(string.lowercase) for i in range(3)) for x in xrange(20)])
        self.unordered_query(connection, query.format(table_name='{table_name}', string_list=string_list))


    # --------------------------------------------------- #
    # ---- Test some predicate and scalar subqueries ---- #
    # --------------------------------------------------- #

    # First predicate subqueries, then scalar subqueries, only with numerical types
    @pytest.mark.parametrize("column1", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("function", [
        'MAX',
        'MIN',
        'SUM',
        'COUNT',
        'AVG'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'int2',
        'real1',
        'real2'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} > (SELECT {function}({column2}) FROM {table_name} WHERE {column2} > 0)''',
        '''SELECT {column1}, (SELECT {function}({column2}) FROM {table_name} WHERE {column2} > 0) AS {column2}_agg FROM {table_name}'''
        ])
    def test_subquery(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))


    # ----------------------- #
    # ---- Test ORDER BY ---- #
    # ----------------------- #

    # test out all the basic sort on one column stuff
    @pytest.mark.parametrize("column1", [
        'int1',
        'real1',
        'date1',
        'timestamp1',
        'string'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} ORDER BY 1''',
        '''SELECT * FROM {table_name} ORDER BY {column1}''',
        '''SELECT * FROM {table_name} ORDER BY {column1} LIMIT 10 OFFSET 10''',
        '''SELECT * FROM {table_name} ORDER BY {column1} NULLS FIRST''',
        '''SELECT * FROM {table_name} ORDER BY {column1} NULLS LAST''',
        '''SELECT * FROM {table_name} ORDER BY {column1} DESC''',
        '''SELECT * FROM {table_name} ORDER BY {column1} DESC NULLS FIRST''',
        '''SELECT * FROM {table_name} ORDER BY {column1} DESC NULLS LAST'''
        ])
    def test_sort(self, column1, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.ordered_query(connection, query.format(table_name='{table_name}', column1=column1))

    # test out compound sorts
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
        '''SELECT * FROM {table_name} ORDER BY {column1}, {column2} '''
        ])
    def test_compound_sort(self, column1, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.ordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2))


    # --------------------------- #
    # ---- Test out GROUP BY ---- #
    # --------------------------- #

    # First test some basic GROUP BY stuff
    # TODO need to think move about the string col.  Maybe have some actual groups in it so this does something
    @pytest.mark.parametrize("column1", [ 
        'string'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'AVG',
        'MAX',
        'MIN',
        'SUM'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'real1'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1}, {function}({column2}) FROM {table_name} GROUP BY {column1}''',
        '''SELECT {column1}, {function}(DISTINCT {column2}) FROM {table_name} GROUP BY {column1}'''
        ])
    def test_basic_group1(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))

    # Try grouping by sign of a numerical col instead of strings.  Also test dates
    @pytest.mark.parametrize("column1", [
        'int1',
        'real1'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'MAX',
        'MIN'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'real1',
        'date1',
        'timestamp1'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT SIGN({column1}) AS sign1, {function}({column2}) FROM {table_name} GROUP BY sign1''',
        '''SELECT SIGN({column1}) AS sign1, {function}(DISTINCT {column2}) FROM {table_name} GROUP BY sign1'''
        ])
    def test_basic_group2(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))

    # Test out the HAVING clause
    @pytest.mark.parametrize("column1", [
        'string'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'MAX',
        'MIN',
        'SUM',
        'AVG'
        ])
    @pytest.mark.parametrize("column2", [
        'int1',
        'real1',
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {function}({column2}) FROM {table_name} GROUP BY {column1} HAVING COUNT({column1}) > 2'''
        ])
    def test_basic_group3(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))
