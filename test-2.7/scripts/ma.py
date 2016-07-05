import multicorn_test
import multicorn_test.mixed_data
from multicorn_test import db_engine, session_factory, session

import pytest


class TestMe(multicorn_test.MulticornBaseTest, multicorn_test.mixed_data.MixedData):

    @pytest.mark.run(order=10)
    def test_create_table(self, db_engine):
        self.create_table(db_engine)

    def test_select_into_table(self, session):
        sqlReturn = session.execute('SELECT count(*) FROM query')
        assert sqlReturn.returns_rows, "Should return some rows"
        numRows = sqlReturn.scalar()
        assert numRows == 0, "Should not be any data in the table"

    @pytest.mark.skip(reason="Keep the table in place for debug")
    @pytest.mark.run(order=-10)
    def test_delete_table(self, session):
        sqlReturn = session.execute('DROP TABLE query')
        assert sqlReturn.returns_rows, "Should not return any rows"
