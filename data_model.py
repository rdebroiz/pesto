import logging
import re

from pesto import quit_with_error
from scope import Scope

try:
    import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")


# char to escape in a regular expression to be taken as literal.
TO_ESCAPE_FOR_RE = r"()[]{}*+?|.^$-\\"
# char to escaped inside [] in a regular expression to be taken as literal.
TO_ESCAPE_INSIDE_BRACKET_FOR_RE = r"\^\-\]\\"


def escape_reserved_re_char(string):
    """
    Escape with a backslash characters reserved by regular expressions
    in the given string.
    """
    # first escape all char that have to be escaped inside []
    # (we're actually putting them inside [])
    to_escape = re.sub("(?P<char>[" + TO_ESCAPE_INSIDE_BRACKET_FOR_RE + "])",
                       r"\\\g<char>",
                       TO_ESCAPE_FOR_RE)
    return re.sub("(?P<char>[" + to_escape + "])",
                  r"\\\g<char>",
                  string)


class DataModel():
    _helpers = {}
    _root = path.Path()
    scopes = {}

    def __init__(self, document):
        self._helpers = document
        try:
            self._root = path.Path(document['__ROOT__']).abspath()
        except KeyError:
            quit_with_error("configuration file must have a '__ROOT__' "
                            "attribute.")
        if(not self._root.exists()):
            quit_with_error("{0}, does not exist ({1} given)."
                            "".format(self._root, document['__ROOT__']))
        try:
            scope_name = document['__SCOPES__']
            self.scopes[scope_name] = [scope for scope in
                                       self._make_scopes(scope_name)]
        except KeyError:
            quit_with_error("configuration file must have a '__SCOPES__' "
                            "attribute.")

    def evaluate(self, string):
        all_evaluated = False
        while(not all_evaluated):
            try:
                match_dolls = re.search(r"\$\{(.*?)\}", string)
            except TypeError:
                logging.error("expression to evaluate is not of type String: "
                              "%s", string)
                raise
            if(match_dolls):
                try:
                    string = self._evaluate_static(string,
                                                   match_dolls.group(1))
                except KeyError:
                    logging.error("unable to evaluate expression: %s",
                                  string)
                    raise
            if(not match_dolls):
                all_evaluated = True
        return string

    def _make_scopes(self, peers):
        for key in peers:
            name = key
            try:
                expression = self.evaluate(peers[key])
            except (TypeError, KeyError):
                quit_with_error("Error in __SCOPES__ definition for {0}"
                                "".format(key))
            values = set()
            for f in self._root.walkfiles():
                try:
                    match = re.search(r".*?" + expression, f)
                except re.error as err:
                    quit_with_error("bad regular expression '{1}' for {0}: "
                                    "{2}".format(key, expression, err))
                if(match):
                    values.add(escape_reserved_re_char(match.group(0)))
            yield Scope(name, expression, sorted(values))

    def _evaluate_static(self, string, to_evaluate):
        return re.sub(r"\$\{" + to_evaluate + r"}",
                      self._get_value_from_helpers(to_evaluate),
                      string)

    def _get_value_from_helpers(self, key):
        try:
            return self._helpers[key]
        except KeyError:
            logging.error("unable to find any key %s in configuration file",
                          key)
