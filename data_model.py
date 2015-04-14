import re
import logging

from pesto import quit_with_error
from scope import Scope
from evaluator import Evaluator

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


class MetaDataModel(type):
    """
    Meta class for DataModel.
    Used to have a 'class property' behavor for the:
    _files, _root and _scopes class attribut.

    i.e. they can't be modified outside DataModel.
    """
    @property
    def files(cls):
        return cls._files

    @property
    def root(cls):
        return cls._root

    @property
    def scopes(cls):
        return cls._scopes


class DataModel(MetaDataModel):

    _files = None
    _root = None
    _scopes = None

    def __init__(self, yaml_doc):
        # Check if the class has already been setup.
        if(DataModel.files is not None and DataModel.root is not None and
           DataModel.scopes is not None):
            logging.warn("DataModel have already been setup:\nroot: %s"
                         "\n%s files\n%s scopes", DataModel.root,
                         len(DataModel.scopes), len(DataModel.scopes))
        # Change helpers class instance attribut so all instances of Evaluators
        # will use it as helpers
        Evaluator.set_helpers(yaml_doc)

        DataModel._set_root(yaml_doc['__ROOT__']).abspath()
        try:
            if(DataModel.scopes is None):
                DataModel.scopes = dict()
            scope_dict = yaml_doc['__SCOPES__']
            DataModel._make_scopes(scope_dict)
        except KeyError:
            quit_with_error("configuration file must have a '__SCOPES__' "
                            "attribute.")

    @classmethod
    def _set_root(cls, root):
        try:
            cls.root = path.Path(root)
        except KeyError:
            quit_with_error("configuration file must have a '__ROOT__' "
                            "attribute.")
        cls.files = root.walkfiles()

    @classmethod
    def _make_scopes(cls, peers):
        evltr = Evaluator()
        for key in peers:
            name = key
            try:
                expression = evltr.evaluate(peers[key])
            except (TypeError, KeyError):
                quit_with_error("Error in __SCOPES__ definition for {0}"
                                "".format(key))
            values = set()
            for f in cls.files:
                try:
                    match = re.search(r".*?" + expression, f)
                except re.error as err:
                    quit_with_error("bad regular expression '{1}' for {0}: "
                                    "{2}".format(key, expression, err))
                if(match):
                    values.add(escape_reserved_re_char(match.group(0)))
            cls.scopes[name] = Scope(name, expression, sorted(values))
