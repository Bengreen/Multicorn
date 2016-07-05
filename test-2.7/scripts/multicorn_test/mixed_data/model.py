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


# TODO: NEED TO REWRITE THIS BIT TO ALLOW NULL
# http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string


class MixedData:

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

    class QueryModelForeign(QueryBase, Base):
        __tablename__ = 'query_for'

    def create_tables(self, db_engine):
        Base.metadata.create_all(db_engine)

    def load_single(self, session):
        temp = self.QueryModelReference(avarchar="jones")
        session.add(temp)
        temp = self.QueryModelReference(avarchar="smith")
        session.add(temp)

    def load(self, session, filename=os.path.dirname(__file__)+'/data.csv'):
        noneValue = '<None>'
        with open(filename, 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
            for row in spamreader:
                print row
                print type(row)
                # Fake a NULL into the CSV as python CSV does not support Null entries
                # http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string
                actualRow = {item[0]: item[1] for item in row.items() if item[1] != noneValue}

                temp = self.QueryModelReference(**actualRow)
                session.add(temp)
