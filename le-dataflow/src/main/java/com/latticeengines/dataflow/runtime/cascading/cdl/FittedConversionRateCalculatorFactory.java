package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public interface FittedConversionRateCalculatorFactory {

    FittedConversionRateCalculator getCalculator(FitFunctionParameters params);

}
