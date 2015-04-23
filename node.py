import logging

ROOT_NAME = "root"


class Node():
    _description = None
    _scope = None
    _name = None
    _cmd = None
    _workers_modifier = None
    _parents = None
    _children = None

    def __init__(self, yaml_doc, previous_node):
        # initialise mutable attributs
        self._parents = set()
        self.children = set()

        from data_model import DataModel
        try:
            self._name = yaml_doc['__NAME__']
        except KeyError:
            logging.error("missing '__NAME__' key")
            raise
        try:
            self._description = yaml_doc['__DESCRIPTION__']
        except KeyError:
            logging.error("missing '__DESCRIPTION__' key")
            raise
        try:
            scope_name = yaml_doc['__SCOPE__']
            self._scope = DataModel.scopes[scope_name]
        except KeyError:
            logging.error("missing '__SCOPE__' key")
            raise
        try:
            self._cmd = yaml_doc['__CMD__']
        except KeyError:
            logging.error("missing '__CMD__' key")
            raise
        try:
            self._parents = yaml_doc['__DEPEND_ON__']
        except KeyError:
            self._parents.add(previous_node.name)
        try:
            self._workers_modifier = yaml_doc['__WORKERS_MODIFIER__']
        except KeyError:
            self._workers_modifier = 1
        try:
            if(self._workers_modifier is not None):
                self._workers_modifier = float(self._workers_modifier)
        except TypeError:
            logging.error("__WORKERS_MODIFIER__ must be castable "
                          "in float")
            raise
        self._scope = DataModel.scopes[scope_name]

    def __str__(self):
        return ("[--\nname: {0},"
                "\ndescription: {1},"
                "\nscope: {2},"
                "\ncmd: {3},"
                "\nworkers_modifier: {4},"
                "\nparents: {5},"
                "\nchildren: {6}"
                "\n--]".format(self._name,
                               self._description,
                               self._scope.name,
                               self._cmd,
                               self._workers_modifier,
                               self._parents,
                               self._children))

    def __repr__(self):
        return self.__str__()

    @property
    def description(self):
        return self._description

    @property
    def scope(self):
        return self._scope

    @property
    def name(self):
        return self._name

    @property
    def cmd(self):
        return self._cmd

    @property
    def workers_modifier(self):
        return self._workers_modifier

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, value):
        self._children = value

    @property
    def parents(self):
        return self._parents

    @parents.setter
    def parents(self, value):
        self._parents = value


class Root(Node):
    def __init__(self):
        self._description = "root"
        self._name = ROOT_NAME
        self._children = set()
        self._parents = set()
        self._workers_modifier = 1
        self._cmd = []

    def __str__(self):
        return ("[--\nname: {0},"
                "\ndescription: {1},"
                "\nchildren: {2}"
                "\n--]".format(self._name,
                               self._description,
                               self._children))
