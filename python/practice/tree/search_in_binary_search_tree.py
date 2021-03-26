class Tree:
    def __init__(self, val, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


class Solution:
    def solve(self, root: Tree, val):
        if root is None:
            return False
        if root.val == val:
            return True

        if root.val > val:
            return self.solve(root.left, val)
        return self.solve(root.right, val)


tree = Tree(val=3,
            left=Tree(val=2),
            right=Tree(val=9,
                       left=Tree(val=7,
                                 left=Tree(val=4),
                                 right=Tree(val=8)),
                       right=Tree(val=12)))

print(Solution().solve(tree, 4))
