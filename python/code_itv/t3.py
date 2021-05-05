class MovingTotal:

    def __init__(self):
        self.n1 = 0
        self.n2 = 0
        self.totals = []

    def append(self, numbers):
        for n in numbers:
            self.totals.append(self.n1 + self.n2 + n)
            self.n1 = self.n2
            self.n2 = n

    def contains(self, total):
        return total in self.totals[2:]


# 1 2 3 4
# 6 9
# add 5
# 9 - 2 + 5
# 6 9 12


if __name__ == "__main__":
    movingtotal = MovingTotal()

    movingtotal.append([1, 2, 3, 4])
    print(movingtotal.totals[2:])
    print(movingtotal.contains(6))
    print(movingtotal.contains(9))
    print(movingtotal.contains(12))
    print(movingtotal.contains(7))

    movingtotal.append([5])
    print(movingtotal.contains(6))
    print(movingtotal.contains(9))
    print(movingtotal.contains(12))
    print(movingtotal.contains(7))
