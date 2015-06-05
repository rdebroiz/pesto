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
        # TODO woulbe great to tell where is the cycle
        # refine graph
        self._transitive_reduction()

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

    def _transitive_reduction(self):
        reducted_graph = nx.DiGraph()
        reducted_graph.add_nodes_from(self._graph.nodes())
        #  get longest path between root and all nodes of the pipeline:
        for node in self._graph.nodes():
            if node == self._root.name:
                continue
            paths = self._find_longest_paths(self._root.name, node)
            # add edges corresponding to those paths to the reducted graph
            for p in paths:
                    end = len(p) - 1
                    for n1, n2 in zip(p[:end], p[1:]):
                        reducted_graph.add_edge(n1, n2)
        self._graph = reducted_graph

    # TODO: This can become very time consuming, look for another algo ?
    def _find_longest_paths(self, src, dest, visited=None, longest_paths=None):
        if visited is None:
            visited = []
        if longest_paths is None:
            longest_paths = [[]]
        # if not src in current_path:
        current_path = visited.copy()
        current_path.append(src)
        # are we at destination ? if yes compare and return
        if src == dest:
            is_longest = False
            for p in longest_paths:
                if len(p) <= len(current_path):
                    is_longest = True
                    longest_paths.append(current_path)
                    if len(p) < len(current_path):
                        longest_paths.remove(p)
                if is_longest:
                    return longest_paths

        # else continue to walk
        else:
            for src, child in self._graph.edges(src):
                longest_paths = self._find_longest_paths(child,
                                                         dest,
                                                         visited=current_path,
                                                         longest_paths=longest_paths)
        return longest_paths

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