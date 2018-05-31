import logging

from collections import OrderedDict

import pandas as pd
import numpy as np
from scipy.stats import linregress
from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.pdversionutil import pd_before_17


class FitFunctionGenerator(State):
    def __init__(self):
        State.__init__(self, "FitFunctionGenerator")
        self._logger = logging.getLogger(name="FitFunctionGenerator")

    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        segmentations = mediator.segmentations
        segments = segmentations[0]['Segments']
        rateChartDf = self.get_rate_dataframe(segments)

        maxRate = self.get_max_rate(rateChartDf)

        decileRateDf = self.get_decile_rate(rateChartDf)

        gamma, alpha, beta = self.interpolate_rate_chart((decileRateDf['rate']))

        structure = OrderedDict()
        structure["alpha"] = alpha
        structure["beta"] = beta
        structure["gamma"] = gamma
        structure["maxRate"] = maxRate

        self.getMediator().fit_function_parameters = structure

    def get_rate_dataframe(self, rateChart):
        rateChartDf = pd.DataFrame.from_records(rateChart)
        return rateChartDf

    def get_max_rate(self, rateChartDf):
        if pd_before_17():
            rateChartDf = rateChartDf.sort('Score', ascending=False)
        else:
            rateChartDf = rateChartDf.sort_values(by='Score', ascending=False)
        maxRateRow = rateChartDf.iloc[0]
        return maxRateRow['Converted'] * 1.0 / maxRateRow['Count']

    def get_decile_rate(self, p1):
        p1['decile'] = p1['Score'].apply(lambda x: int((x - 1) / 10 + 1))
        p1_decile = p1.groupby(by='decile')['Count', 'Converted'].sum()
        p1_decile['rate'] = p1_decile['Converted'] / p1_decile['Count']

        p1_decile.sort_index(inplace=True, ascending=False)

        return p1_decile

    def linear_fit(self, x, y):
        x = x.reshape(x.shape[0], )
        slope, intercept, r_value, p_value, ste_err = linregress(x, y)
        alpha = slope
        beta = intercept
        if r_value >= -0.001 or p_value > 0.5:
            alpha = 0.0
            beta = np.mean(y)
        diff = np.sum(np.square(np.exp(beta + x.reshape(x.shape[0], ) * alpha) - np.exp(y)) * np.asarray(
            range(x.shape[0] + 1, 1, -1)))
        return alpha, beta, diff

    def interpolate_rate_chart(self, rateArray):
        rateArray = np.asarray(rateArray)
        gammaList = [-0.99999, 0, 100000]
        # add a small value to avoid np.log(0)
        rateArray += 1e-7
        rateArray = np.log(np.asarray([x for x in rateArray if x > np.mean(rateArray) * 0.2]))
        gridSearchResults = {}
        for gridSearchDepth in range(8):
            for gamma in gammaList:
                if gamma in gridSearchResults:
                    continue
                decile = np.log(np.arange(1, rateArray.shape[0] + 1) + gamma).reshape(rateArray.shape[0], 1)
                gridSearchResults[gamma] = self.linear_fit(decile, rateArray)
            if gridSearchResults[gammaList[1]][2] >= gridSearchResults[gammaList[0]][2] \
                    and gridSearchResults[gammaList[2]][2] >= gridSearchResults[gammaList[1]][2]:
                gammaList = [gammaList[0], 0.5 * (gammaList[0] + gammaList[1]), gammaList[1]]
            elif gridSearchResults[gammaList[1]][2] <= gridSearchResults[gammaList[0]][2] \
                    and gridSearchResults[gammaList[2]][2] <= gridSearchResults[gammaList[1]][2]:
                gammaList = [gammaList[1], 0.5 * (gammaList[1] + gammaList[2]), gammaList[2]]
            else:
                gammaList = [0.5 * (gammaList[1] + gammaList[0]), gammaList[1], 0.5 * (gammaList[1] + gammaList[2])]
        minDiff = min([x[2] for x in gridSearchResults.values()])
        minGamma = [x for x, y in gridSearchResults.items() if np.abs(y[2] - minDiff) < 1e-6][0]
        return (minGamma, gridSearchResults[minGamma][0], gridSearchResults[minGamma][1])
