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

class Variable:
    """Represents a variable (i.e. an intrinsic, changeable parameter of a computational graph).
    """

    def __init__(self, initial_value=None):
        """Construct Variable

        Args:
          initial_value: The initial value of this variable
        """
        self.value = initial_value
        self.consumers = []

        # Append this variable to the list of variables in the currently active default graph
        _default_graph.variables.append(self)


class Placeholder:
    """Represents a placeholder node that has to be provided with a value
           when computing the output of a computational graph
        """

    def __init__(self):
        self.comsumers = []

        _default_graph.placeholders.append(self)


class Graph:

    def __init__(self):
        self.operations = []
        self.placeholders = []
        self.variables = []

    def as_default(self):
        global _default_graph
        _default_graph = self


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


#############################################
# Create a new graph
Graph().as_default()

# Create variables
A = Variable([[1, 0], [0, -1]])
b = Variable([1, 1])

# Create placeholder
x = Placeholder()

# Create hidden node y
y = Matmul(A, x)

# Create output node z
z = Add(y, b)

session = Session()
output = Session.run(z, {
    x: [1, 2]
})

print(output)