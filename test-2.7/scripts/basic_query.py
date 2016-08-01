from multicorn_test import MulticornBaseTest
import pytest
import os
import io
import random
import string
from collections import OrderedDict


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

    @pytest.fixture(scope="class")
    def table_columns(self, request):
        cols = OrderedDict()

        cols['tinyint_a'] = 'SMALLINT'
        cols['tinyint_b'] = 'SMALLINT'
        cols['smallint_a'] = 'SMALLINT'
        cols['smallint_b'] = 'SMALLINT'
        cols['int_a'] = 'INTEGER'
        cols['int_b'] = 'INTEGER'
        cols['bigint_a'] = 'BIGINT'
        cols['bigint_b'] = 'BIGINT'
        cols['float_a'] = 'REAL'
        cols['float_b'] = 'REAL'
        cols['double_a'] = 'DOUBLE PRECISION'
        cols['double_b'] = 'DOUBLE PRECISION'
        cols['decimal_a'] = 'DECIMAL(20,10)'
        cols['decimal_b'] = 'DECIMAL(20,10)'
        cols['timestamp_a'] = 'TIMESTAMP'
        cols['timestamp_b'] = 'TIMESTAMP'
        cols['date_a'] = 'DATE'
        cols['date_b'] = 'DATE'
        cols['string_a'] = 'TEXT'
        cols['string_b'] = 'TEXT'
        cols['varchar_a'] = 'VARCHAR(100)'
        cols['varchar_b'] = 'VARCHAR(100)'
        cols['char_a'] = 'CHAR(100)'
        cols['char_b'] = 'CHAR(100)'
        cols['boolean_a'] = 'BOOLEAN'
        cols['boolean_b'] = 'BOOLEAN'
        cols['binary_a'] = 'BYTEA'
        cols['binary_b'] = 'BYTEA'

        return cols

    @classmethod
    def sample_io(cls):
        return open('test_data.csv')


    # --------------------------------------------------------------------------
    # SQL Test Suite
    # --------------------------------------------------------------------------

    # ------------------------ #
    # --- Failing queries ---- #
    # ------------------------ #
    @pytest.mark.basic
    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate random ORDER")('''SELECT * from {table_name} order by RANDOM()''')
        ])
    def test_failing_ordered(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
        #import pdb; pdb.set_trace()
        self.ordered_query(connection, query)

    @pytest.mark.basic
    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate random WHERE")('''SELECT * from {table_name} WHERE RANDOM() < 0.5'''),
        pytest.mark.xfail(reason="deliberate random ORDER and LIMIT")('''SELECT * from {table_name} ORDER BY RANDOM() LIMIT 5''')
        ])
    def test_failing_unordered(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query)

    @pytest.mark.basic
    @pytest.mark.parametrize("query", [
        pytest.mark.xfail(reason="deliberate overflow of a small int (2 bytes)")('''SELECT smallint_a * 1000000 from {table_name}'''),
        pytest.mark.xfail(reason="deliberate overflow of an integer (4 bytes)")('''SELECT int_a * 10000000000 from {table_name}'''),
        pytest.mark.xfail(reason="deliberate overflow of an big int (8 bytes)")('''SELECT bigint_a * 10000000000 from {table_name}''')
        ])
    def test_failing_overflow(self, connection, query, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query)

    # ------------------------- #
    # ---- Very basic SQL  ---- #
    # ------------------------- #
    @pytest.mark.basic
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
    @pytest.mark.basic
    @pytest.mark.parametrize("column", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
        'timestamp_a',
        'date_a',
        'string_a',
        'varchar_a',
        'char_a',
        'boolean_a',
        'binary_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column} FROM {table_name}''',
        '''SELECT {column} AS alias FROM {table_name}''',
        '''SELECT DISTINCT {column} FROM {table_name}'''
        ])
    def test_select(self, column, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column=column))

    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
        'timestamp_a',
        'date_a',
        'string_a',
        'varchar_a',
        'char_a',
        'boolean_a',
        'binary_a'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
        'timestamp_a',
        'date_a',
        'string_a',
        'varchar_a',
        'char_a',
        'boolean_a',
        'binary_a'
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
    @pytest.mark.basic
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
        ])
    @pytest.mark.parametrize("operator", [
        '+',
        '-',
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_b', # only test col + col arithmetic for tinyint to avoid overflows
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

    # Test multiply against specified constants (to avoid overflow issues)
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("operator", [
        '*'
        ])
    @pytest.mark.parametrize("column2", [
        '1',
        '0',
        '-1',
        '0.123',
        '-0.234'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1} {operator} {column2} FROM {table_name}''',
        ])
    def test_select_arithmetic2(self, column1, column2, operator, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2, operator=operator))

    # Test divide against specified constants (to avoid div by zero issues)
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
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
    def test_select_arithmetic3(self, column1, column2, operator, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2, operator=operator))

    # Test exponentiation with only positive ints to avoid complex numbers and that whole mess
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
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
    def test_select_arithmetic4(self, column1, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, column2=column2))


    # ------------------------------- #
    # ---- Test SELECT functions ---- #
    # ------------------------------- #
    # test out some functions on columns, check they're working ok
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("function", [
        'ABS',
        'FLOOR',
        'CEILING',
        'SIGN'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {function}({column1}) FROM {table_name}''',
        '''SELECT DISTINCT {function}({column1}) FROM {table_name}'''
        ])
    def test_select_functions1(self, column1, function, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function))

    # Test DIV and POWER on ints
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT DIV({column1}, 2) FROM {table_name}''',
        '''SELECT POWER({column1}, 1) FROM {table_name}'''
        ])
    def test_select_functions2(self, column1, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1))

    # And test POWER on floats
    @pytest.mark.parametrize("column1", [
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT POWER({column1}, 2) FROM {table_name}'''
        ])
    def test_select_functions3(self, column1, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1))


    # ------------------------------ #
    # ---- Test WHERE operators ---- #
    # ------------------------------ #

    # First for numerical types
    @pytest.mark.basic
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
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
        'tinyint_b',
        'smallint_b',
        'int_b',
        'bigint_b',
        'float_b',
        'double_b',
        'decimal_b',
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
        'date_a',
        'timestamp_a'
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
        'date_b',
        'timestamp_b',
        "'2016-07-27'",
        "'1986-12-19 12:00:00'",
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} {operator} {column2}''',
        '''SELECT * FROM {table_name} WHERE NOT {column1} {operator} {column2}'''
        ])
    def test_where_operators_times(self, column1, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, operator=operator, column2=column2))

    # Then for string types
    @pytest.mark.basic
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("operator", [
        '=',
        '!=',
        '<>'
        ])
    @pytest.mark.parametrize("column2", [
        'string_b',
        'varchar_b',
        'char_b'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} {operator} {column2}''',
        '''SELECT * FROM {table_name} WHERE NOT {column1} {operator} {column2}'''
        ])
    def test_where_operators_strings(self, column1, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, operator=operator, column2=column2))


    # -------------------------------------- #
    # ---- Test WHERE logical operators ---- #
    # -------------------------------------- #

    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("operator", [
        'AND',
        'OR'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_b',
        'smallint_b',
        'int_b',
        'bigint_b',
        'float_b',
        'double_b',
        'decimal_b'
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
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("function", [
        'ABS',
        'FLOOR',
        'CEILING',
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
        'tinyint_b',
        'smallint_b',
        'int_b',
        'bigint_b',
        'float_b',
        'double_b',
        'decimal_b'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {function}({column1}) {operator} {column2}'''
        ])
    def test_where_logical_operators(self, column1, function, operator, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, operator=operator, column2=column2))


    # -------------------------------------------- #
    # ---- Test text specific WHERE functions ---- #
    # -------------------------------------------- #

    # Test out LIKE and ILIKE, I predict this section will be the source of many many problems...
    # NOTE: anywhere there's a double %% it will be translated to a single % in the final SQL statement, due to python string formatting, so use your imagination
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("regex", [
        "'%%'",
        "'say%%'",
        "'want%%'",
        "'%%say'",
        "'%%want'",
        "'sa_'",
        "'wan_'",
        "'_ay'",
        "'_ant'",
        "'_ay%%'",
        "'_ant%%'",
        "'%%sa_'",
        "'%%wan_'",
        "'_a_'",
        "'_an_'"
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} LIKE {regex}''',
        '''SELECT * FROM {table_name} WHERE {column1} ILIKE {regex}'''
        ])
    def test_string_like(self, column1, regex, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, regex=regex))

    # Test out the IN operator
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} IN {string_list}''',
        '''SELECT * FROM {table_name} WHERE NOT {column1} IN {string_list}'''
        ])
    def test_string_in(self, column1, query, connection, foreign_table, ref_table_populated, for_table_populated):
        #string_list = "('%s')" % "','".join([''.join(random.choice(string.lowercase) for i in range(3)) for x in xrange(20)])
        string_list = "('be look', 'or people', 'at also', 'and we', 'look well', 'people your', 'good like', 'even only', 'time into', 'also work')"
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, string_list=string_list))

    # Test out SIMILAR TO
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("regex", [
        "'%%'",
        "'say'",
        "'want'",
        "'say%%'",
        "'want%%'",
        "'%%say'",
        "'%%want'",
        "'%%(say|want)%%'",
        "'%%(say|want)'",
        "'(say|want)%%'"
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} SIMILAR TO {regex}'''
        ])
    def test_string_similar_to(self, column1, regex, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, regex=regex))

    # And test out proper POSIX REGEX
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("regex", [ # This list should be enough to flag any big issues,but it's very basic and shoudl be expanded
        "'say say'",
        "'he want'",
        "'say*'",
        "'want*'",
        "'^ay*'",
        "'^ant*'",
        "'^a*'",
        "'^(say|want)*'",
        "'(say|want)*'"
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT * FROM {table_name} WHERE {column1} ~ {regex}'''
        ])
    def test_string_regex(self, column1, regex, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, regex=regex))


    # --------------------------------------------------- #
    # ---- Test some predicate and scalar subqueries ---- #
    # --------------------------------------------------- #

    # First predicate subqueries, then scalar subqueries, only with numerical types
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("function", [
        'MAX',
        'MIN',
        'SUM',
        'COUNT',
        'AVG'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_b',
        'smallint_b',
        'int_b',
        'bigint_b',
        'float_b',
        'double_b',
        'decimal_b'
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
    @pytest.mark.basic
    @pytest.mark.parametrize("column1", [
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
        'timestamp_a',
        'date_a',
        'string_a',
        'varchar_a',
        'char_a'
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
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a',
        'timestamp_a',
        'date_a',
        'string_a',
        'varchar_a',
        'char_a',
        'boolean_a'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_b',
        'smallint_b',
        'int_b',
        'bigint_b',
        'float_b',
        'double_b',
        'decimal_b',
        'timestamp_b',
        'date_b',
        'string_b',
        'varchar_b',
        'char_b',
        'boolean_b'
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
    @pytest.mark.parametrize("column1", [ 
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'AVG',
        'MAX',
        'MIN',
        'SUM'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {column1}, {function}({column2}) FROM {table_name} GROUP BY {column1}''',
        '''SELECT {column1}, {function}(DISTINCT {column2}) FROM {table_name} GROUP BY {column1}'''
        ])
    def test_basic_group1(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))

    # Try grouping by sign of a numerical col instead of strings.  Also test dates
    @pytest.mark.parametrize("column1", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'MAX',
        'MIN'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_b',
        'smallint_b',
        'int_b',
        'bigint_b',
        'float_b',
        'double_b',
        'decimal_b'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT SIGN({column1}) AS sign1, {function}({column2}) FROM {table_name} GROUP BY sign1''',
        '''SELECT SIGN({column1}) AS sign1, {function}(DISTINCT {column2}) FROM {table_name} GROUP BY sign1'''
        ])
    def test_basic_group2(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))

    # Test out the HAVING clause
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'MAX',
        'MIN',
        'SUM',
        'AVG'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT {function}({column2}) FROM {table_name} GROUP BY {column1} HAVING COUNT({column1}) > 2'''
        ])
    def test_basic_group3(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))


    # ---------------------------------- #
    # ---- Test out WINDOW functions --- #
    # ---------------------------------- #

    # Test window functions, which are basically de-aggregated group bys
    @pytest.mark.parametrize("column1", [
        'string_a',
        'varchar_a',
        'char_a'
        ])
    @pytest.mark.parametrize("function", [
        'COUNT',
        'AVG',
        'MAX',
        'MIN',
        'SUM'
        ])
    @pytest.mark.parametrize("column2", [
        'tinyint_a',
        'smallint_a',
        'int_a',
        'bigint_a',
        'float_a',
        'double_a',
        'decimal_a'
        ])
    @pytest.mark.parametrize("query", [
        '''SELECT *, {function}({column2}) OVER (PARTITION BY {column1}) FROM {table_name}'''
        ])
    def test_basic_window(self, column1, function, column2, query, connection, foreign_table, ref_table_populated, for_table_populated):
        self.unordered_query(connection, query.format(table_name='{table_name}', column1=column1, function=function, column2=column2))
