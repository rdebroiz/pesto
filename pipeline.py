from log import quit_with_error

try:
    import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")
import logging
import concurrent
import collections


class PipelineError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


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
    _nodes = None
    _root = Root()

    def __init__(self, yaml_documents):
        if Pipeline.node is not None:
            logging.warning("Pipeline have already been setup.")
        else:
            Pipeline = {Pipeline.root.name, Pipeline.root}
            self._build_from_documents(yaml_documents, Pipeline.root)
            try:
                Pipeline._transitive_reduction()
                Pipeline._resolve_children()
                Pipeline._backend_edge_detection()
            except PipelineError as err:
                quit_with_error("Error detected in pipeline.\n{0}".format(err))

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
    def _transitive_reduction(cls):
        for node_x in cls.nodes.values():
            for node_y in cls.nodes.values():
                for node_z in cls.nodes.values():
                    # delete edge xz if edges xy and yz exist
                    if(node_x.name in node_z.parents and
                       node_x.name in node_y.parents and
                       node_y.name in node_z.parents):
                        node_z.parents.remove(node_x.name)

    @classmethod
    def _resolve_children(cls):
        for node_name_1 in cls.nodes:
            for node_name_2 in cls.nodes:
                if node_name_1 == node_name_2:
                    continue
                else:
                    node1 = cls.nodes[node_name_1]
                    node2 = cls.nodes[node_name_2]
                    if node_name_1 in node2.parents:
                        node1.children.add(node_name_2)

    @classmethod
    def _backend_edge_detection(cls):
        visited = set()
        for node in cls.walk(cls.root):
            visited.add(node.name)
            for n in visited:
                if n in node.children:
                    raise PipelineError("circular dependence: "
                                        "{0} depend on {1} which is "
                                        "already one of his children"
                                        "".format(n, node.name))

    @classmethod
    def walk(cls, node):
        """ iterate tree in pre-order depth-first search order """
        yield node
        for child in node.children:
            for n in cls.walk(cls.nodes[child]):
                yield n


# class PipelineExecutor():
#     def print_progression(self):
#         print("pouette")

#     def execute_node(self, node):

# class ThreadedPipelineExecutor(PipelineExecutor):
#     _max_workers = 0
#     _futures = None

#     def __init__(self, max_workers):
#         self._max_workers = max_workers
#         self._futures = dict()

#     def execute():
#         for node in Pipeline.walk(Pipeline.root):
#             max_workers = self._max_workers * node.max_workers
#             with concurrent.future.ThreadPoolExecutor(max_workers) as ex:
#                 for scope_value in node.scope.values:                    
#                 self._futures[scope_value] = ex.submit()

