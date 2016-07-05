import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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


@pytest.fixture
def session(request, session_factory):
    print("Creating Session")
    sess = session_factory()

    def fin():
        sess.commit()
        sess.close()
        print("Commited Session")

    request.addfinalizer(fin)
    return sess


def test_noop(db_engine, username):
    print(username)
    assert 1


def test_noop2(db_engine):
    print("Noop2")
    assert 1


class TestBen:
    @classmethod
    def callme(cls):
        print ("callme called!")

    def test_method1(self, session):
        result = session.execute('SELECT')
        assert result.returns_rows, "should return rows"
        assert result.rowcount == 1, "Should return 1 row"
        assert len(result.keys()) == 0, "Should not return any columns"

    def test_method2(self, session):
        print ("test_method1 called")
