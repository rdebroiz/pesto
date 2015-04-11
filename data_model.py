import logging
import re

from pesto import quit_with_error
from scope import Scope

try:
    from path import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")


# char to escaped in a regular expression outside []
TO_ESCAPE_FOR_RE = r"()[]{}*+?|.^$-\\"
# char to escaped in a regular expression inside []
TO_ESCAPE_IN = r"\^\-\]\\"


def escape_reserved_re_char(string):
    """
    Escape with a backslash characters reserved by regular expressions
    in the given string.
    """
    # first escape all char that have to be escaped inside []
    # (we're actually putting them inside [])
    to_escape = re.sub("(?P<char>[" + TO_ESCAPE_IN + "])",
                       r"\\\g<char>",
                       TO_ESCAPE_FOR_RE)
    return re.sub("(?P<char>[" + to_escape + "])",
                  r"\\\g<char>",
                  string)


class DataModel():
    _helpers = {}
    _root = path()
    scopes = []

    def __init__(self, document):
        self._helpers = document

        try:
            self._root = path(document['__ROOT__']).abspath()
        except KeyError:
            quit_with_error("configuration file must have a '__ROOT__' "
                            "attribute.")
        if(not self._root.exists()):
            quit_with_error("{0}, does not exist ({1} given)."
                            "".format(self._root, document['__ROOT__']))

        try:
            self.scopes = [scope for scope in
                           self._make_scopes(document['__SCOPES__'])]
        except KeyError:
            quit_with_error("configuration file must have a '__SCOPES__' "
                            "attribute.")

    def resolve(self, string):
        all_evaluated = False
        while(not all_evaluated):
            try:
                match_dolls = re.search(r"\$\{(.*?)\}", string)
            except TypeError:
                logging.error("expression to resolve is not of type String: "
                              "%s", string)
                raise
            if(match_dolls):
                try:
                    string = self._resolve_static(string,
                                                  match_dolls.group(1))
                except KeyError:
                    logging.error("unable resolve expression: %s",
                                  string)
                    raise
            if(not match_dolls):
                all_evaluated = True
        return string

    def _make_scopes(self, peers):
        for key in peers:
            name = key
            try:
                expression = self.resolve(peers[key])
            except (TypeError, KeyError):
                quit_with_error("Error in __SCOPES__ definition for {0}"
                                "".format(key))
            values = set()
            for f in self._root.walkfiles():
                try:
                    match = re.search(r".*?" + expression, f)
                except re.error as err:
                    quit_with_error("bad regular expression r'{1}' for {0}: "
                                    "{2}".format(key, expression, err))
                if(match):
                    values.add(escape_reserved_re_char(match.group(0)))
            yield Scope(name, expression, sorted(values))

    def _resolve_static(self, string, to_resolve):
        return re.sub(r"\$\{" + to_resolve + r"}",
                      self._get_value_from_helpers(to_resolve),
                      string)

    def _get_value_from_helpers(self, key):
        try:
            return self._helpers[key]
        except KeyError:
            logging.error("unable to find any key %s in configuration file",
                          key)
