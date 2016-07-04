#!/usr/bin/env python

# Run with py.test xxxx.py
# OR
# py.test --junitxml results.xml xxxx.py
# OR make directly executable

# if __name__ == '__main__':
#     unittest.main()

# suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
# unittest.TextTestRunner(verbosity=2).run(suite)


# --------- Testing Preparation -----------

# from udv_test_lib import UdvBaseTest
import multicorn_test
import mixed_data

import pytest
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import text


class TestMe(multicorn_test.MulticornBaseTest, mixed_data.MixedData):
    # @pytest.mark.run(order=10)
    # def test_create_table(self):
    #     s = text('''
    #       create table basetable (
    #       id integer,
    #       adate date,
    #       atimestamp timestamp,
    #       anumeric numeric,
    #       avarchar varchar
    #       )
    #     ''')
    #     sqlReturn = self.conn.execute(s)
    #     self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))

    @pytest.mark.run(order=10)
    def test_create_table(self):
        # self.metadata.create_all(self.engine)
        self.create_table()

    @pytest.mark.run(order=20)
    def test_load_table(self):
        # self.metadata.create_all(self.engine)
        self.load()

    @pytest.mark.skip(reason="Keep the table in place for debug")
    @pytest.mark.run(order=-10)
    def test_delete_table(self):
        sqlReturn = self.conn.execute('DROP TABLE query')
        self.assertFalse(sqlReturn.returns_rows, msg="Return is %s" % (sqlReturn))
