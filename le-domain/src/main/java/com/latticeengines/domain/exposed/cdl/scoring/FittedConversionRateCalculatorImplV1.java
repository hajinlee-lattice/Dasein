package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;

import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public class FittedConversionRateCalculatorImplV1
        implements FittedConversionRateCalculator, Serializable {
    private static final long serialVersionUID = -661135563911121355L;
    private FitFunctionParameters params;

    public FittedConversionRateCalculatorImplV1(FitFunctionParameters params) {
        validateParameters(params);
        this.params = params;
    }

    public double calculate(int percentile) {

        // calculation based on data scientist's algorithm
        double alpha = params.getAlpha();
        double beta = params.getBeta();
        double gamma = params.getGamma();
        double maxRate = params.getMaxRate();

        if (Math.abs(alpha) < 1e-6) {
            return Math.exp(beta);
        }

        double mappedPercentile = (-percentile + 105) * 0.1;
        if (mappedPercentile >= 1) {
            return Math.exp(beta + Math.log(mappedPercentile + gamma) * alpha);
        }
        double rateAtOne = Math.exp(beta + Math.log(1 + gamma) * alpha);
        return (maxRate > rateAtOne)
                ? rateAtOne + (1 - mappedPercentile) * 10 * (maxRate - rateAtOne) / 5 : rateAtOne;
    }
}
