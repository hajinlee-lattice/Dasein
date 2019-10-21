package com.latticeengines.domain.exposed.cdl.scoring;

import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public interface FittedConversionRateCalculatorFactory {

    FittedConversionRateCalculator getCalculator(FitFunctionParameters params);

    double calculate(Double avgRawScore, long totalEvent);

}
