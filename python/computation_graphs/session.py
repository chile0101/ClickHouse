import numpy as np
from operation import Operation


class Session:
    """Represents a particular execution of a computational graph.
    """

    def run(self, operation, feed_dict={}):
        """Computes the output of an operation

                Args:
                  operation: The operation whose output we'd like to compute.
                  feed_dict: A dictionary that maps placeholders to values for this session
                """

        # Perform a post-order traversal of the graph to bring the nodes into the right order
        nodes_postorder = traverse_postorder(operation)


def traverse_postorder(operation):
    """Performs a post-order traversal, returning a list of nodes
        in the order in which they have to be computed

        Args:
           operation: The operation to start traversal at
        """

    nodes_postorder = []

    def recurse(node):
        if isinstance(node, Operation):
            for input_node in node.input_nodes:
                recurse(input_node)

        nodes_postorder.append(node)

    recurse(operation)

    return nodes_postorder
