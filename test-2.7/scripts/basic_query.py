import multicorn_test
import multicorn_test.mixed_data
from multicorn_test import db_engine, session_factory, session

import pytest


class TestBasicQuery(multicorn_test.MulticornBaseTest, multicorn_test.mixed_data.MixedData):

    @pytest.mark.run(order=10)
    def test_create_table(self, db_engine):
        self.create_tables(db_engine)

    @pytest.mark.skip(reason="Count is wrong when debugging")
    @pytest.mark.run(order=-10)
    def test_delete_table(self, session):
        sqlReturn = session.execute('DROP TABLE query_ref, query_for')
        assert not sqlReturn.returns_rows, "Should not return any rows"

    @pytest.mark.run(order=20)
    def test_load(self, session):
        self.load(session)

    @pytest.mark.run(order=20)
    def test_copy_ref_to_for(self, session):
        self.copy_ref_to_for(session)

    # @pytest.mark.skip(reason="Count is wrong when debugging")
    @pytest.mark.run(order=15)
    def test_select_into_query_ref(self, session):
        sqlReturn = session.execute('SELECT count(*) FROM query_ref')
        assert sqlReturn.returns_rows, "Should return some rows"
        numRows = sqlReturn.scalar()
        assert numRows == 0, "Should not be any data in the table"

    def test_ordered_query(self, session):
        query_text = '''SELECT * from {0} order by avarchar'''
        self.ordered_query(session, query_text)

    @pytest.mark.run(order=20)
    def test_create_extension_multicorn(self, session):
        sqlReturn = session.execute('''CREATE EXTENSION multicorn''')
        assert not sqlReturn.returns_rows, "Should not return rows"

    @pytest.mark.run(order=-20)
    def test_drop_extension_multicorn(self, session):
        sqlReturn = session.execute('''DROP EXTENSION multicorn''')
        assert not sqlReturn.returns_rows, "Should not return rows"
