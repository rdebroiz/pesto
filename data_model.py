import re
import logging
from pprint import pformat

from log import quit_with_error
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

    @files.setter
    def files(self, value):
        self._files = value

    @property
    def root(cls):
        return cls._root

    @root.setter
    def root(self, value):
        self._root = value

    @property
    def scopes(cls):
        return cls._scopes

    @scopes.setter
    def scopes(self, value):
        self._scopes = value

    @property
    def document_path(self):
        return self._document_path

    @document_path.setter
    def document_path(self, value):
        self._document_path = value


class DataModel(metaclass=MetaDataModel):
    _files = None
    _root = None
    _scopes = None
    _document_path = None

    def __init__(self, yaml_doc, yaml_doc_dir, scope_to_override):
        # Check if the class has already been setup.
        if(DataModel.files is not None and DataModel.root is not None and
           DataModel.scopes is not None):
            logging.warn("DataModel have already been setup:\nroot: %s"
                         "\n%s files\n%s scopes", DataModel.root,
                         len(DataModel.scopes), len(DataModel.scopes))
        DataModel.document_path = yaml_doc_dir
        # Change helpers class instance attribut so all instances of Evaluators
        # will use it as helpers
        from evaluator import Evaluator
        # update yaml_doc with scopte_to_override before setting helpers.
        # if scope_to_override in yaml_doc:
        yaml_doc.update(scope_to_override)
        Evaluator.set_helpers(yaml_doc)
        try:
            DataModel._set_root(yaml_doc['__ROOT__'])
        except KeyError:
            logging.error("configuration file must have a '__ROOT__' "
                          "attribute.")
        except (OSError, KeyError, TypeError):
            quit_with_error("unable to build data model. "
                            "bad key: '__ROOT__'")
        try:
            if(DataModel.scopes is None):
                DataModel.scopes = dict()
            scope_dict = yaml_doc['__SCOPES__']
            # check if scope to override are ok.
            for scope in scope_to_override:
                if scope not in scope_dict:
                    quit_with_error("Unable to find overrided scope "
                                    "'\033[91m{}\033[0m\033[1m'".format(scope))
            scope_dict.update(scope_to_override)
            DataModel._make_scopes(scope_dict)
            logging.debug("Scopes:\n%s", pformat(DataModel.scopes))
        except KeyError:
            logging.error("configuration file must have a '__SCOPES__' "
                          "attribute.")
            quit_with_error("unable to build data model. "
                            "bad key: '__SCOPES__'")

    @classmethod
    def _set_root(cls, root):
        from evaluator import Evaluator
        evltr = Evaluator()        
        root = evltr.evaluate(root)
        cls.root = path.Path(root).abspath()
        try:
            cls.files = sorted(cls.root.walkfiles())
            logging.debug("files:\n%s", pformat(cls.files))
        except OSError:
            logging.error("no such directory: ('%s')", cls.root)
            raise

    @classmethod
    def _make_scopes(cls, peers):
        from evaluator import Evaluator
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
