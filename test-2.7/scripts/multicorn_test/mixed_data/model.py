import csv
import os

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Date, DateTime, Numeric
from sqlalchemy.sql import text

from sqlalchemy.ext.declarative import declarative_base

import pytest


# Base = declarative_base()

# TODO: Move this into the class (if possible)
Base = declarative_base()


class MixedData:
    # Constructor does not seem to be allowed in py.test
    # def __init__(self):
    #     print("Constructor")
    #     super(MixedData, self).__init__()

    class QueryBase(object):
        id = Column(Integer, primary_key=True)
        adate = Column(Date)
        atimestamp = Column(DateTime)
        anumeric = Column(Numeric)
        avarchar = Column(String)

        def __init__(self, **kwargs):
            for (k, v) in kwargs.items():
                setattr(self, k, v)

    class QueryModelReference(QueryBase, Base):
        __tablename__ = 'query_ref'

    # class QueryModelForeign(QueryBase, Base):
    #     __tablename__ = 'query_for'

    # TODO: Make this into a property
    @classmethod
    def ref_table_name(cls):
        # return cls.QueryModelReference.__tablename__
        return 'query_ref'

    # TODO: Make this into a property
    @classmethod
    def for_table_name(cls):
        return 'query_for'
        # return cls.QueryModelForeign.__tablename__

    @pytest.fixture(scope="class")
    def ref_table(self, request, session_factory, db_engine):
        self.exec_sql(session_factory, '''
            CREATE TABLE IF NOT EXISTS {0} (
                id integer PRIMARY KEY,
                adate date,
                atimestamp timestamp,
                anumeric numeric,
                avarchar varchar
            );'''.format(self.ref_table_name()))

        Base.metadata.reflect(bind=db_engine, only=[self.ref_table_name()])

        ref_table = Base.metadata.tables[self.ref_table_name()]

        def fin():
            Base.metadata.remove(ref_table)
            self.exec_sql(session_factory, 'DROP TABLE {0}'.format(self.ref_table_name()))

        request.addfinalizer(fin)

        return ref_table

    def test_ref_table(self, session_factory, ref_table):
        (keys, values) = self.exec_return_value(session_factory, 'SELECT * FROM {0}'.format(self.ref_table_name()))
        import pdb; pdb.set_trace()
        assert len(values) == 0, 'Expecting %s to be empty, found %s' % (self.ref_table_name(), values)
    # assert 0, "Received keys=%s, values=%s" % (keys, values)
        # session = session_factory()
        # connection = session.connection()
        # tableObj = Base.metadata.tables['ben']
        # newRow = tableObj.insert().values(id=5, avarchar='hello')
        # session.execute(newRow)
        # session.commit()
        # assert 0, 'Fail here to catch an error and show output'

    @pytest.fixture(scope="function")
    def ref_table_populated(self, request, session_factory, ref_table, filename=os.path.dirname(__file__)+'/data.csv'):
        session = session_factory()
        noneValue = '<None>'
        with open(filename, 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
            for row in spamreader:
                # Fake a NULL into the CSV as python CSV does not support Null entries
                # http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string
                actualRow = {item[0]: item[1] for item in row.items() if item[1] != noneValue}

                temp = ref_table.insert().values(**actualRow)
                session.execute(temp)
        assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)
        session.commit()
        session.close()

        def fin():
            self.exec_sql(session_factory, 'DELETE FROM {0}'.format(self.ref_table_name()))

        request.addfinalizer(fin)
