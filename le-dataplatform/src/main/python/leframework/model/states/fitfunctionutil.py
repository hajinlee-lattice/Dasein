import numpy as np


class FitFunctionUtil():

    @staticmethod
    def getMappedRate(x, beta, alpha, gamma, maxRate):
        mapped_x = (-x + 105) * 0.1
        if np.abs(alpha) < 1e-6:
            return np.exp(beta)
        if mapped_x + gamma < 1e-6:
            return maxRate
        return min(maxRate, np.exp(beta + np.log(mapped_x + gamma) * alpha))
