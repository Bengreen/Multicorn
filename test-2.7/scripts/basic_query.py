import multicorn_test
import multicorn_test.mixed_data
from multicorn_test import db_engine, session_factory, session

import pytest


class TestBasicQuery(multicorn_test.MulticornBaseTest, multicorn_test.mixed_data.MixedData):

    @pytest.mark.run(order=10)
    def test_create_table(self, db_engine):
        self.create_tables(db_engine)

    @pytest.mark.skip(reason="Keep the table in place for debug")
    @pytest.mark.run(order=-10)
    def test_delete_table(self, session):
        sqlReturn = session.execute('DROP TABLE query_ref, query_for')
        assert sqlReturn.returns_rows, "Should not return any rows"

    def test_load_single(self, session):
        self.load_single(session)

    def test_load(self, session):
        self.load(session)

    @pytest.mark.skip(reason="Count is wrong when debugging")
    def test_select_into_query_ref(self, session):
        sqlReturn = session.execute('SELECT count(*) FROM query_ref')
        assert sqlReturn.returns_rows, "Should return some rows"
        numRows = sqlReturn.scalar()
        assert numRows == 0, "Should not be any data in the table"
