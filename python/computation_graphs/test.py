from graph import Graph
from placeholder import Placeholder
from variable import Variable
from operation import Add, Matmul
from session import Session

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
