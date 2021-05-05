from typing import List


class SubrectangleQueries:

    def __init__(self, rectangle: List[List[int]]):
        self.h = len(rectangle)
        self.w = len(rectangle[0])
        self.data = [item for sublist in rectangle for item in sublist]

    def updateSubrectangle(self, row1: int, col1: int, row2: int, col2: int, newValue: int) -> None:
        for i in range(self.indexOf(row1, col1), self.indexOf(row2, col2) + 1):
            self.data[i] = newValue


    def indexOf(self, row, col):
        return (row - 1) * self.h + (col - 1) * self.w + 1

    def getValue(self, row: int, col: int) -> int:
        return self.data[self.indexOf(row, col)]

s = SubrectangleQueries([[3,9,4],[5,6,10],[3,3,3]])
s.updateSubrectangle(1,1,1,1,5)
s.updateSubrectangle(0,0,1,0,6)
print(s.data)
print(s.getValue(0,1))

