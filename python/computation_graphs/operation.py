class Operation:
    """Represents a graph node that performs a computation.

        An `Operation` is a node in a `Graph` that takes zero or
        more objects as input, and produces zero or more objects
        as output.
        """

    def __init__(self, input_nodes=[]):
        self.input_nodes = input_nodes
        self.customers = []

        for input_node in input_nodes:
            input_node.consumers.append(self)

        _default_graph.operations.append(self)

    def compute(self):
        """Computes the output of this operation.
                "" Must be implemented by the particular operation.
                """
        pass


class Add(Operation):
    """Returns x + y element-wise.
    """

    def __init__(self, x, y):
        """Construct add

        Args:
          x: First summand node
          y: Second summand node
        """

        super(Add, self).__init__([x, y])

    def compute(self, x_value, y_value):
        """Compute the output of the add operation

                Args:
                  x_value: First summand value
                  y_value: Second summand value
                """

        return x_value + y_value


class Matmul(Operation):
    """Multiplies matrix a by matrix b, producing a * b.
        """

    def __init__(self, a, b):
        """Construct matmul

        Args:
          a: First matrix
          b: Second matrix
        """
        super().__init__([a, b])

    def compute(self, a_value, b_value):
        """Compute the output of the matmul operation

                Args:
                  a_value: First matrix value
                  b_value: Second matrix value
                """

        return a_value.dot(b_value)


