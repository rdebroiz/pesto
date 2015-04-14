from pipeline import Pipeline
from data_model import DataModel
import logging


class Node():
    _description = None
    _scope = None
    _name = None
    _cmd = None
    _dependencies = None
    _workers_modifier = None
    _parents = [Pipeline.root.name]
    _children = []

    def __init__(self, yaml_doc, previous_node=None):
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
            self._dependencies = yaml_doc['__DEPENDENCIES__']
        except KeyError:
            if(previous_node is not None):
                self._dependencies = [previous_node.name]
        try:
            self._workers_modifier = float(['__WORKERS_MODIFIER__'])
            self._scope = DataModel.scopes[scope_name]
        except KeyError:
            self._workers_modifier = 1
        except TypeError:
            logging.error("__WORKERS_MODIFIER__ must be castable "
                          "in float")

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
    def dependencies(self):
        return self._dependencies

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
