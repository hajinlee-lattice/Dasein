package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public interface FittedConversionRateCalculator {
    public double calculate(int percentile);
}
