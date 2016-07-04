import pytest


def pytest_addoption(parser):
    parser.addoption("--username", action="store", default="udvtest", help="username: username to access the DB")
    parser.addoption("--password", action="store", default="pass", help="password: password to access the DB")
    parser.addoption("--db", action="store", default="udv", help="db: db to access the DB")


# @pytest.fixture
# def username(request):
#     return request.config.getoption("--username")
#
#
# @pytest.fixture
# def password(request):
#     return request.config.getoption("--password")
#
#
# @pytest.fixture
# def db(request):
#     return request.config.getoption("--db")


@pytest.fixture(scope='class')
def params(request):
    request.cls.username = request.config.getoption("--username")
    request.cls.password = request.config.getoption("--password")
    request.cls.db = request.config.getoption("--db")


def pytest_cmdline_preparse(args):
    args[:] = ["-k", "\"not UdvBaseTest\""] + args
