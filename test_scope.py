import scope
import logging
import pytest


@pytest.fixture()
def init_logging():
    """
        Fixture to init a console handler
        to see DBUG message during tests

        "py.test -s"
    """
    print("")
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(levelname)s]: %(message)s")
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


@pytest.yield_fixture()
def init_scope():
    init_logging()
    scope_ = scope.Scope("test", r"^test$", ['/t/1', '/t/2'])
    yield scope_


def test_init(init_scope):
    assert init_scope.name == "test"
    assert init_scope.expression == r"^test$"
    assert init_scope.values == ['/t/1', '/t/2']