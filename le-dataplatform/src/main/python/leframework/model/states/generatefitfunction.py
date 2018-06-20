import logging

from collections import OrderedDict

import pandas as pd
import numpy as np
from scipy.stats import linregress
from scipy.stats import spearmanr
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
        structure = OrderedDict()

        try:
            rateChartDf = self.get_rate_dataframe(segments)
            maxRate = self.get_max_rate(rateChartDf)
            avgRate = self.get_avg_rate(rateChartDf)
            decileRateDf = self.get_decile_rate(rateChartDf)
            gamma, alpha, beta = self.rate_chart_fit((decileRateDf['rate']), decileRateDf['decile'], avgRate)
        except Exception as exp:
            self._logger.exception("Caught Exception while generating fit function: " + str(exp))
            alpha, beta, gamma, maxRate = self.get_default_fit_function_params()

        structure["alpha"] = alpha
        structure["beta"] = beta
        structure["gamma"] = gamma
        structure["maxRate"] = maxRate
        structure["version"] = "v2"

        self.getMediator().fit_function_parameters = structure

    def get_default_fit_function_params(self):
        return 0.0, 0.0, 0.0, 0.0

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
        p1_decile.reset_index(inplace=True)
        return p1_decile

    def get_avg_rate(self, rateChartDf):
        return rateChartDf['Converted'].sum() * 1.0 / rateChartDf['Count'].sum()

    def linear_fit(self, xx, yy):
        outlierThld = 2.0

        params = []
        slope, intercept, r_value, p_value, std_err = linregress(xx, yy)
        diff = np.sum(np.square(np.exp(intercept + xx.reshape(xx.shape[0], ) * slope) - np.exp(yy)) * np.asarray(
            range(xx.shape[0] + 1, 1, -1)))

        params.append([slope, intercept, r_value, p_value, diff])

        # find outliers
        sq_err = np.sqrt(np.average(np.square(slope * xx + intercept - yy)))
        ind_err = np.abs(np.subtract(slope * xx + intercept, yy))

        matches = np.argwhere(ind_err <= outlierThld * sq_err).T[0]
        matches = np.asarray(list(set(matches).union(set({0, 1}))))

        if matches.shape[0] < yy.shape[0]:
            x_matches = xx[matches]
            y_matches = yy[matches]
            x_matches = x_matches.reshape(x_matches.shape[0], )
            slope, intercept, r_value, p_value, std_err = linregress(x_matches, y_matches)
            diff = np.sum(np.square(np.exp(intercept + xx.reshape(xx.shape[0], ) * slope) - np.exp(yy)) * np.asarray(
                range(xx.shape[0] + 1, 1, -1)))
            params.append([slope, intercept, r_value, p_value, diff])

        minid = np.argmin(np.asarray([x[4] for x in params]))

        bestParam = params[minid]

        alpha = bestParam[0]
        beta = bestParam[1]

        if bestParam[2] >= -0.001 or bestParam[3] > 0.5:
            alpha = 0.0
            beta = 0.0

        diff = np.sum(np.square(np.exp(beta + xx.reshape(xx.shape[0], ) * alpha) - np.exp(yy)) * np.asarray(
            range(xx.shape[0] + 1, 1, -1)))

        return alpha, beta, diff

    # Grid search over gamma, and returns gamma, alpha and beta that best fit the function :
    # rate = beta * (decile + gamma) ** alpha
    def rate_chart_fit(self, rateArray, decileArray, avgRate):

        if avgRate < 1e-6 or rateArray[0] <= avgRate:
            return (0.0, 0.0, np.log(max(avgRate, 1e-6)))

        rateArray = np.asarray(rateArray)
        decileArray = np.asarray(decileArray)[::-1]

        decileArray = decileArray[~np.isnan(rateArray)]
        rateArray = rateArray[~np.isnan(rateArray)]

        if rateArray.shape[0] < 3:
            return (0.0, 0.0, np.log(avgRate))

        liftArray = rateArray / avgRate
        decile = decileArray

        idx = np.argwhere(liftArray > 0.01)

        if idx.shape[0] >= 3:
            liftArray = liftArray[idx]
            decile = decile[idx]
        else:
            liftArray = liftArray[:3]
            decile = decile[:3]

        rho, pval = spearmanr(decile, liftArray)

        if rho >= -0.001 or pval > 0.5:
            return (0.0, 0.0, np.log(avgRate))

        gammaList = [-0.9, 0, 100]

        gridSearchResults = {}
        for gridSearchDepth in range(8):
            for gamma in gammaList:
                if gamma in gridSearchResults:
                    continue
                logdecile = np.log(decile + gamma).reshape(decile.shape[0], )
                logliftArray = np.log(liftArray + 1e-6).reshape(liftArray.shape[0], )
                gridSearchResults[gamma] = self.linear_fit(logdecile, logliftArray)
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
        return (minGamma, gridSearchResults[minGamma][0], gridSearchResults[minGamma][1] + np.log(avgRate))
