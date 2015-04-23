from log import quit_with_error

try:
    import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")
from pprint import pformat


class MetaPipeline(type):
    @property
    def nodes(cls):
        return cls._nodes

    @nodes.setter
    def nodes(cls, value):
        cls._nodes = value

    @property
    def root(cls):
        return cls._root


class Pipeline(metaclass=MetaPipeline):

    from node import Root
    _nodes = dict()
    _root = Root()
    _nodes[_root.name] = _root

    def __init__(self, yaml_documents):
        self._build_from_documents(yaml_documents, Pipeline.root)
        for node_name in Pipeline.nodes:
            node = Pipeline.nodes[node_name]
            Pipeline._create_dependences(node)
        print(pformat(Pipeline.nodes))
        circular_dependence = Pipeline._cycle_detection()
        if(circular_dependence):
            quit_with_error("circular dependence detected in the pipeline:"
                            "\n\t{} depend on {} which is already one of his "
                            "children".format(circular_dependence[0],
                                              circular_dependence[1]))

    def _build_from_documents(self, documents, previous_node):
        from node import Node
        from yaml_io import YamlIO
        from evaluator import Evaluator
        from data_model import DataModel
        evltr = Evaluator()
        for doc in documents:
            if('__FILE__' in doc):
                filename = path.Path(evltr.evaluate(doc['__FILE__']))
                if(not filename.isabs()):
                    filename = DataModel.document_path / filename
                d = YamlIO.load_all_yaml(filename)
                previous_node = self._build_from_documents(d, previous_node)
            else:
                node = Node(doc, previous_node)
                previous_node = node
                Pipeline.nodes[node.name] = node
        return previous_node

    @classmethod
    def _create_dependences(cls, node):
        for name in cls.nodes:
            node_to_resove_with = cls.nodes[name]
            if node.name in node_to_resove_with.parents:
                node.children.add(name)

    @classmethod
    def _cycle_detection(cls):
        visited = set()
        for node in cls.walk(Pipeline.root):
            visited.add(node.name)
            for n in visited:
                if n in node.children:
                    return [n, node.name]
        return False

    @classmethod
    def walk(cls, node):
        """ iterate tree in pre-order depth-first search order """
        yield node
        for child in node.children:
            for n in cls.walk(Pipeline.nodes[child]):
                yield n
