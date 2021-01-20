class Placeholder:
    """Represents a placeholder node that has to be provided with a value
           when computing the output of a computational graph
        """

    def __init__(self):
        self.comsumers = []

        _default_graph.placeholders.append(self)
