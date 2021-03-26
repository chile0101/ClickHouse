from typing import List


class Tree:
    def __init__(self, val, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


class Solution:
    def exits(self, root: Tree, val):
        if root is None:
            return False
        if root.val == val:
            return True

        if root.val > val:
            return self.exits(root.left, val)
        return self.exits(root.right, val)

    def preorder_traversal(self, root: Tree) -> List[int]:
        # root left right
        res = []
        if root:
            res.append(root.val)
            res += self.preorder_traversal(root.left)
            res += self.preorder_traversal(root.right)

        return res

    def inorder_traversal(self, root: Tree) -> List[int]:
        # left root right
        res = []
        if root:
            res = self.inorder_traversal(root.left)
            res.append(root.val)
            res += self.inorder_traversal(root.right)
        return res

    def postorder_traversal(self, root: Tree) -> List[int]:
        # left right root
        res = []
        if root:
            res += self.postorder_traversal(root.left)
            res += self.postorder_traversal(root.right)
            res.append(root.val)
        return res


tree = Tree(val=3,
            left=Tree(val=2),
            right=Tree(val=9,
                       left=Tree(val=7,
                                 left=Tree(val=4),
                                 right=Tree(val=8)),
                       right=Tree(val=12)))

# print(Solution().solve(tree, 4))
# print(Solution().preorder_traversal(tree))
# print(Solution().inorder_traversal(tree))
print(Solution().postorder_traversal(tree))
