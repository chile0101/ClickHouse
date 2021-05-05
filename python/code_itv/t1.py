import pandas
import numpy


class M:
    def gmtkf(self):
        return 1.609

    def mtk(self, miles):
        return self.gmtkf() * miles


class N(M):
    def gmtkf(self):
        return 1.852

    def pf(self):
        print(self.gmtkf(), super().gmtkf())


print(N().mtk(1))
