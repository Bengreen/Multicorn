import csv
import os

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Date, DateTime, Numeric
from sqlalchemy.sql import text

from sqlalchemy.ext.declarative import declarative_base

import pytest


Base = declarative_base()


class QueryModel(Base):
    __tablename__ = 'query'
    id = Column(Integer, primary_key=True)
    adate = Column(Date)
    atimestamp = Column(DateTime)
    anumeric = Column(Numeric)
    avarchar = Column(String)

    def __init__(self, **kwargs):
        for (k, v) in kwargs.items():
            setattr(self, k, v)


# NEED TO REWRITE THIS BIT TO ALLOW NULL
# http://stackoverflow.com/questions/11379300/csv-reader-behavior-with-none-and-empty-string

class MixedData:
    def create_table(self, db_engine):
        Base.metadata.create_all(db_engine)

    def load(self, filename=os.path.dirname(__file__)+'/data.csv'):
        with open(filename, 'rb') as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
            for row in spamreader:
                print row
                temp = QueryModel(**row)
                self.session.add(temp)
