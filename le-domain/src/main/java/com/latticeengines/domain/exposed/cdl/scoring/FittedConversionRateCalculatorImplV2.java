package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;

import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public class FittedConversionRateCalculatorImplV2
        implements FittedConversionRateCalculator, Serializable {
    private static final long serialVersionUID = -6982693393807845252L;
    private FitFunctionParameters params;

    public FittedConversionRateCalculatorImplV2(FitFunctionParameters params) {
        validateParameters(params);
        this.params = params;
    }

    @Override
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

        if (mappedPercentile + gamma < 1e-6) {
            return maxRate;
        }

        return Math.min(maxRate, Math.exp(beta + Math.log(mappedPercentile + gamma) * alpha));

    }
}
