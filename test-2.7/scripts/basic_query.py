import multicorn_test
import multicorn_test.mixed_data
from multicorn_test import db_engine, session_factory, session

import pytest


class TestBasicQuery(multicorn_test.MulticornBaseTest, multicorn_test.mixed_data.MixedData):
    @pytest.mark.run(order=10)
    def test_create_table(self, db_engine):
        self.create_tables(db_engine)

    @pytest.mark.skipif(pytest.config.getvalue("teardown") == "False", reason="--teardown was disabled: (==False)")
    @pytest.mark.run(order=-10)
    def test_delete_table(self, session):
        self.exec_no_return(session, '''DROP TABLE query_ref, query_for''')

    @pytest.mark.run(order=20)
    def test_create_extension_multicorn(self, session):
        self.exec_no_return(session, '''CREATE EXTENSION multicorn''')

    @pytest.mark.skipif(pytest.config.getvalue("teardown") == "False", reason="--teardown was disabled: (==False)")
    @pytest.mark.run(order=-20)
    def test_drop_extension_multicorn(self, session):
        self.exec_no_return(session, '''DROP EXTENSION multicorn''')

    @pytest.mark.run(order=30)
    def test_create_helper_function(self, session):
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

    # @pytest.mark.skipif(debug, reason="Leave tables in place when debugging")
    @pytest.mark.skipif(pytest.config.getvalue("teardown") == "False", reason="--teardown was disabled: (==False)")
    @pytest.mark.run(order=-30)
    def test_drop_helper_function(self, session):
        self.exec_no_return(session, '''DROP function create_foreign_server()''')

    @pytest.mark.run(order=100)
    def test_load(self, session):
        self.load(session)

    @pytest.mark.run(order=110)
    def test_copy_ref_to_for(self, session):
        self.copy_ref_to_for(session)

    # @pytest.mark.skip(reason="Count is wrong when debugging")
    @pytest.mark.run(order=15)
    def test_select_into_query_ref(self, session):
        sqlReturn = session.execute('SELECT count(*) FROM query_ref')
        assert sqlReturn.returns_rows, "Should return some rows"
        numRows = sqlReturn.scalar()
        assert numRows == 0, "Should not be any data in the table"

    # --------------------------------------------------------------------------
    # Tests start here
    # --------------------------------------------------------------------------

    @pytest.mark.parametrize("query", ['''SELECT * from {0} order by avarchar''', '''SELECT * from {0} order by anumeric'''])
    def test_ordered_query(self, session, query):
        # query_text = '''SELECT * from {0} order by avarchar'''
        self.ordered_query(session, query)
