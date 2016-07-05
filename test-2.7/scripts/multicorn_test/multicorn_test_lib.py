import pytest
#
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker


class MulticornBaseTest:
    # @classmethod
    # def callme(cls):
    #     print ("callme called!")

    @pytest.mark.run(order=20)
    def test_connection(self, session):
        result = session.execute('SELECT')
        assert result.returns_rows, "should return rows"
        assert result.rowcount == 1, "Should return 1 row"
        assert len(result.keys()) == 0, "Should not return any columns"
