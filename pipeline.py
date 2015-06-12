from log import quit_with_error
import logging
from pprint import pformat

try:
    import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")
try:
    import networkx as nx
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")

from node import Root


class Pipeline():
    _root = Root()
    _graph = nx.DiGraph()
    _nodes = None

    def __init__(self, yaml_documents):
        # init with root
        self._nodes = {self._root.name: self._root}
        self._graph.add_node(self._root.name)
        # build graph
        self._build_nodes_from_documents(yaml_documents)
        self._build_edges()
        if self._cycle_detection():
            quit_with_error("Pipeline can't be cyclic")
        # refine graph
        import ipdb; ipdb.set_trace()
        self._thin()

    def _build_nodes_from_documents(self, documents):
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
                self._build_nodes_from_documents(d)
            else:
                try:
                    node = Node(doc, self._graph.nodes())
                except:
                    quit_with_error("Unable to build node from: "
                                    "{}".format(pformat(doc)))
                self._graph.add_node(node.name)
                self._nodes[node.name] = node

    def _build_edges(self):
        for node in self._nodes:
            for parent in self._nodes[node].parents:
                self._graph.add_edge(parent, node)

    def _cycle_detection(self):
        have_cycle = False
        for cycle in nx.simple_cycles(self._graph):
            have_cycle = True
            logging.error("Cycle found: "
                          "%s", pformat(cycle))
        return have_cycle

    def _thin(self):
        for n in self._graph.nodes():
            for cur_p in self._graph.predecessors(n):
                for p in self._graph.predecessors(n):
                    if cur_p is p:
                        continue
                    else:
                        if cur_p in nx.ancestors(self._graph, p):
                            self._graph.remove_edge(cur_p, n)
                            break

    def walk(self, node):
        # TODO there must have a better way to do it.
        # yield node
        for n in nx.topological_sort(self._graph,
                                     nx.descendants(self._graph, node.name)):
            yield self._nodes[n]

    @property
    def root(self):
        return self._root

    @property
    def nodes(self):
        return self._nodes