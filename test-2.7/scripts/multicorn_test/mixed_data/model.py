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
    def reference_table_name(cls):
        return cls.QueryModelReference.__tablename__

    # TODO: Make this into a property
    @classmethod
    def foreign_table_name(cls):
        return 'query_for'
        # return cls.QueryModelForeign.__tablename__

    def create_tables(self, session_factory):
        # TODO: Change this to not explicity create the table based on Python model, but instead generate python model based on SQL.... Then consider to use a single session throughout the whole process rather than a new session for each transaction
        session = session_factory(autoflush=False)
        db_engine = session.get_bind()
        Base.metadata.create_all(db_engine)

    def load(self, session_factory, filename=os.path.dirname(__file__)+'/data.csv'):
        session = session_factory()
        noneValue = '<None>'
        with open(filename, 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
            for row in spamreader:
                # Fake a NULL into the CSV as python CSV does not support Null entries
                # http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string
                actualRow = {item[0]: item[1] for item in row.items() if item[1] != noneValue}

                temp = self.QueryModelReference(**actualRow)
                session.add(temp)
        assert session.is_active, 'Query did not complete and expects a rollback: %s' % (query)
        session.commit()
        session.close()
