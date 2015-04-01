from soda_yaml import evaluate_yaml_expression

import pytest


@pytest.fixture()
def init():
    import builtins
    builtins.SODA_DATA_STRUCTURE = {'SCOPE_1': 'scope_1.*?/',
                                    'SCOPE_2':
                                    '(${SCOPE_2_DIGIT}|${SCOPE_2_LETTER})',
                                    'SCOPE_2_DIGIT': 'scope_2_\\d/',
                                    'SCOPE_2_LETTER': 'scope_2_[a-zA-Z]/',
                                    'SCOPE_3': 'scope_3-\\w',
                                    'SCOPE_4': 'scope_4.*?$',
                                    'SCOPE2_DYNAMIC': '?{SCOPE_2}',
                                    '__ROOT__': 'tests/test_data',
                                    '__SCOPES__': {'SCOPE_1': '${SCOPE_1}',
                                                   'SCOPE_2': '${SCOPE_2}',
                                                   'SCOPE_2_DIGIT':
                                                   '${SCOPE_2_DIGIT}',
                                                   'SCOPE_2_LETTER':
                                                   '${SCOPE_2_LETTER}',
                                                   'SCOPE_3': '${SCOPE_3}',
                                                   'SCOPE_4': '${SCOPE_4}',
                                                   '__ROOT__': '${__ROOT__}'}}


@pytest.yield_fixture()
def init_for_dynamic():
    init()
    import builtins
    builtins.SODA_FILES_IN_ROOT = ['tests_data/data/scope_1/scope_2_1/'
                                   'scope_3-b_scope_4_a',
                                   'tests_data/data/scope_1/scope_2_a/'
                                   'scope_3-b_scope_4_a',
                                   'tests_data/data/scope_1/scope_2_b/'
                                   'scope_3-b_scope_4_a']
    yield('tests_data/data/scope_1/scope_2_b/')


def test_evaluate_simple_string(init):
    to_test = evaluate_yaml_expression("This is a simple string")
    assert to_test == "This is a simple string"


def test_evaluate_static_expr(init):
    to_test = evaluate_yaml_expression("${SCOPE_2_DIGIT} is a static "
                                       "expression")
    assert to_test == "scope_2_\\d/ is a static expression"


def test_evaluate_nested_static_expr(init):
    to_test = evaluate_yaml_expression("${SCOPE_2} is a nested static "
                                       "expression")
    assert to_test == ("(scope_2_\\d/|scope_2_[a-zA-Z]/) is a nested static "
                       "expression")


def test_evaluate_dynamic_expr(init_for_dynamic):
    to_test = evaluate_yaml_expression("?{SCOPE_2_LETTER} is a dynamic "
                                       "expression",
                                       current_expr=init_for_dynamic)
    assert to_test == ("scope_2_b/ is a dynamic expression")


def test_evaluate_static_expr_nested_in_dynamic(init_for_dynamic):
    to_test = evaluate_yaml_expression("?{SCOPE_2} is a dynamic expression "
                                       "with a static one nested in it",
                                       current_expr=init_for_dynamic)
    assert to_test == ("scope_2_b/ is a dynamic expression with a static one "
                       "nested in it")


def test_evaluate_dynamic_expr_nested_in_static_expr(init_for_dynamic):
    to_test = evaluate_yaml_expression("${SCOPE2_DYNAMIC} is a static "
                                       "expression with a dynamic one nested "
                                       "in it",
                                       current_expr=init_for_dynamic)
    assert to_test == ("scope_2_b/ is a static expression with a dynamic one "
                       "nested in it")
