import pytest


def pytest_addoption(parser):
    parser.addoption("--username", action="store", default="udvtest", help="username: username to access the DB")
    parser.addoption("--password", action="store", default="pass", help="password: password to access the DB")
    parser.addoption("--db", action="store", default="udv", help="db: db to access")
    parser.addoption("--teardown", action="store", default="True", help="teardown: remove test elements at end of test")
    parser.addoption("--fdw", action="store", default="multicorn.sqlalchemyfdw.SqlAlchemyFdw", help="fdw: Foreign Data Wrapper to test against")
    parser.addoption("--fdw_options", action="store", default="tablename '{ref_table_name}', password '{password}'", help="fdw_options: Options to be passed directly into FDW")


@pytest.fixture(scope='module')
def username(request):
    """
    provide the username via a command line parameter to the test
    """
    return request.config.getoption("--username")


@pytest.fixture(scope='module')
def password(request):
    return request.config.getoption("--password")


@pytest.fixture(scope='module')
def db(request):
    return request.config.getoption("--db")


@pytest.fixture(scope='module')
def fdw(request):
    return request.config.getoption("--fdw")


@pytest.fixture(scope='module')
def fdw_options(request):
    return request.config.getoption("--fdw_options")


# @pytest.fixture(scope='class')
# def params(request):
#     request.cls.username = request.config.getoption("--username")
#     request.cls.password = request.config.getoption("--password")
#     request.cls.db = request.config.getoption("--db")


# def pytest_cmdline_preparse(args):
#     """
#     Do not run built in tests from the test framework against the framework itself
#     """
#     args[:] = ["-k", "\"not MulticornBaseTest\""] + args
