import data_model
import logging
import pytest
import path


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
def init_data_model():
    init_logging()
    document = {'SCOPE_1': 'scope_1.*?/',
                'SCOPE_2': '(${SCOPE_2_DIGIT}|${SCOPE_2_LETTER})',
                'SCOPE_2_DIGIT': 'scope_2_\\d/',
                'SCOPE_2_LETTER': 'scope_2_[a-zA-Z]/',
                'SCOPE_3': 'scope_3-\\w',
                'SCOPE_4': 'scope_4.*?$',
                'SCOPE2_DYNAMIC': '?{SCOPE_2}',
                '__ROOT__': 'tests_data',
                '__SCOPES__': {'SCOPE_1': '${SCOPE_1}',
                               'SCOPE_2': '${SCOPE_2}',
                               'SCOPE_2_DIGIT':
                               '${SCOPE_2_DIGIT}',
                               'SCOPE_2_LETTER':
                               '${SCOPE_2_LETTER}',
                               'SCOPE_3': '${SCOPE_3}',
                               'SCOPE_4': '${SCOPE_4}',
                               '__ROOT__': '${__ROOT__}'}}
    model = data_model.DataModel(document)
    yield model


def test_escape_reserved_re_char(init_logging):
    for char in data_model.TO_ESCAPE_FOR_RE:
        assert data_model.escape_reserved_re_char(char) == "\\" + char


def test_init(init_data_model):
    assert init_data_model._helpers ==\
        {'SCOPE_1': 'scope_1.*?/',
         'SCOPE_2': '(${SCOPE_2_DIGIT}|${SCOPE_2_LETTER})',
         'SCOPE_2_DIGIT': 'scope_2_\\d/',
         'SCOPE_2_LETTER': 'scope_2_[a-zA-Z]/',
         'SCOPE_3': 'scope_3-\\w',
         'SCOPE_4': 'scope_4.*?$',
         'SCOPE2_DYNAMIC': '?{SCOPE_2}',
         '__ROOT__': 'tests_data',
         '__SCOPES__': {'SCOPE_1': '${SCOPE_1}',
                        'SCOPE_2': '${SCOPE_2}',
                        'SCOPE_2_DIGIT':
                        '${SCOPE_2_DIGIT}',
                        'SCOPE_2_LETTER':
                        '${SCOPE_2_LETTER}',
                        'SCOPE_3': '${SCOPE_3}',
                        'SCOPE_4': '${SCOPE_4}',
                        '__ROOT__': '${__ROOT__}'}}

    assert init_data_model._root == path.Path('tests_data').abspath()
    assert len(init_data_model.scopes) == 7


def test_evaluate_static(init_data_model):
    ans = init_data_model._evaluate_static('${SCOPE_1}', "SCOPE_1")
    assert ans == "scope_1.*?/"

    ans = init_data_model._evaluate_static('$${SCOPE_1}}', "SCOPE_1")
    assert ans == "$scope_1.*?/}"


def test_evaluate_nested(init_data_model):
    ans = init_data_model.evaluate('${SCOPE_2}')
    assert ans == "(scope_2_\\d/|scope_2_[a-zA-Z]/)"
