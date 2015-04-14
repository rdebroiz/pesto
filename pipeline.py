from node import Node
from yaml_io import YamlIO
from evaluator import Evaluator
from pesto import quit_with_error

try:
    import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")


class MetaPipeline(type):
    @property
    def nodes(cls):
        return cls._nodes

    @property
    def root(cls):
        return cls._root


class Pipeline():

    class Root(Node):
        _description = "root"
        _name = "root"
        _children = []

    _nodes = None
    _root = Root()

    def __init__(self, yaml_documents):
        self._build_from_documents
        for node_name in Pipeline.nodes:
            node = Pipeline.nodes[node_name]
            Pipeline._create_dependences(node)
        circular_dependence = Pipeline._cycle_detection()
        if(circular_dependence):
            quit_with_error("circular dependence detected in the pipeline:"
                            "\n{} depend on {} which is already one of his "
                            "children".format(circular_dependence[0],
                                              circular_dependence[1]))

    def _build_from_documents(self, documents):
        evltr = Evaluator()
        for doc in documents:
            if('__FILE__' in doc):
                try:
                    filename = path.Path(evltr.evaluate(doc['__FILE__']))
                    self._build_from_documents(YamlIO.load_all_yaml(filename))
                except (KeyError, TypeError):
                    quit_with_error("Unable to build pipeline from "
                                    "{0}".format(doc))
            else:
                try:
                    Pipeline.add_node(Node(doc))
                except(KeyError, TypeError):
                    quit_with_error("Unable to build node from "
                                    "{0}".format(doc))

    @classmethod
    def _create_dependences(cls, node):
        for name in cls._nodes:
            node_to_resove_with = cls._nodes[name]
            if node.name in node_to_resove_with.dependencies:
                node.add_children([name])
            if name in node.dependencies:
                node.add_parents[name]

    @classmethod
    def _cycle_detection(cls):
        visited = []
        for node in cls.walk(Pipeline.root):
            visited += node.name
            for n in visited:
                if n in node.children:
                    return [n, node.name]
        return False

    @classmethod
    def add_node(cls, node):
        cls._nodes[node.name] = node

    @classmethod
    def walk(cls, node):
        """ iterate tree in pre-order depth-first search order """
        yield node
        for child in node.children:
            for n in cls.walk(child):
                yield n
