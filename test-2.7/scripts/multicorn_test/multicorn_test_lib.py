import pytest
from sqlalchemy.sql import text


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

    def exec_no_return(self, session, query):
        returnVal = session.execute(query)
        assert not returnVal.returns_rows, "Not expecting any rows"

    def copy_ref_to_for(self, session):
        self.exec_no_return(session, '''INSERT INTO {1} SELECT * from {0}'''.format(self.reference_table_name(), self.foreign_table_name()))
        # query =
        # returnVal = session.execute()
        # assert not return_Val.returns_rows, "Not expecting any rows"

    def ordered_query(self, session, query):
        query_ref = query.format(self.reference_table_name())
        query_for = query.format(self.foreign_table_name())
        print("RUNNING ref:%s and for:%s" % (query_ref, query_for))

        return_ref = session.execute(query_ref)
        return_for = session.execute(query_for)
        print("ref:%s and for:%s" % (return_ref, return_for))
        print("looking at ref")

        assert return_ref.returns_rows == return_for.returns_rows, "Expecting ref and for to both be same for returns_rows"

        if not return_ref.returns_rows:
            return

        assert return_ref.rowcount == return_for.rowcount, "Expecting ref and for to have same number of rows"

        result_ref = return_ref.fetchall()
        result_for = return_for.fetchall()

        for (row_ref, row_for) in zip(result_ref, result_for):
            assert row_ref == row_for, 'Rows should match %s == %s' % (row_ref, row_for)
