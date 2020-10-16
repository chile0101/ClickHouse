class A:
    class_name = "class A"

    def __init__(self, x, y):
        self.x = x
        self.y = y

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y

    @x.setter
    def x(self, x):
        self._x = x

    @y.setter
    def y(self, y):
        self._y = y

    def foo(self, z):
        print(f"executing foo {self, z}")

    @classmethod
    def class_foo(cls, x):
        print(f"class name {cls.class_name}")
        print(f"exuting class foo {cls, x}")

    @staticmethod
    def static_foo(x):
        print(f"exuting static foo{x}")


a = A(1, 2)

# a.foo(3)
A.class_foo(3)
# A.static_foo(3)
