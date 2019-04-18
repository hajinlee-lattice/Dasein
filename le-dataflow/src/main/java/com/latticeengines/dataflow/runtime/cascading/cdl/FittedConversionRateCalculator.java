package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public interface FittedConversionRateCalculator {
    double calculate(int percentile);

    default void validateParameters(FitFunctionParameters params) {
        if (Double.isInfinite(params.getAlpha()) || Double.isNaN(params.getAlpha())) {
            throw new IllegalArgumentException(
                    "Invalid fit function parameter alpha: " + params.getAlpha());
        }
        if (Double.isInfinite(params.getBeta()) || Double.isNaN(params.getBeta())) {
            throw new IllegalArgumentException(
                    "Invalid fit function parameter beta: " + params.getBeta());
        }
        if (Double.isInfinite(params.getGamma()) || Double.isNaN(params.getGamma())) {
            throw new IllegalArgumentException(
                    "Invalid fit function parameter gamma: " + params.getGamma());
        }
        if (Double.isInfinite(params.getMaxRate()) || Double.isNaN(params.getMaxRate())) {
            throw new IllegalArgumentException(
                    "Invalid fit function parameter maxRate: " + params.getMaxRate());
        }
    }
}
