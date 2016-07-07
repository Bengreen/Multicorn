from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest


@pytest.fixture(scope="module")
def db_engine(request, username, password, db):
    print("Connecting to PG Engine")
    engine = create_engine('postgresql://%s:%s@localhost:5432/%s' % (username, password, db), echo=True)

    def fin():
        engine.dispose()
        print("Closed PG Engine")

    request.addfinalizer(fin)
    return engine  # provide the fixture value


@pytest.fixture(scope="module")
def session_factory(request, db_engine):
    print("Creating and binding Session factory")
    Session = sessionmaker(bind=db_engine)

    return Session  # provide the fixture value


@pytest.fixture(scope="function")
def session(request, session_factory):
    print("Creating Session")
    sess = session_factory()

    def fin():
        sess.commit()
        sess.close()
        print("Commited Session")

    request.addfinalizer(fin)
    return sess
