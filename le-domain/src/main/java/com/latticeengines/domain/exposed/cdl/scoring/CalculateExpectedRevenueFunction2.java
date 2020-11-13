package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.EVFitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public class CalculateExpectedRevenueFunction2 implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenueFunction2.class);

    private static final long serialVersionUID = 8549465250465151489L;
    private FittedConversionRateCalculator probabilityFitter;
    private FittedConversionRateCalculator predictedRevenueFitter;

    public CalculateExpectedRevenueFunction2(String evFitFunctionParamsStr) {
        EVFitFunctionParameters evFitFunctionParameters = parseEVFitFunctionParams(evFitFunctionParamsStr);
        FitFunctionParameters probFitParams = evFitFunctionParameters.getProbabilityParameters();
        FitFunctionParameters predictedRevenueFitParams = evFitFunctionParameters.getRevenueParameters();
        probabilityFitter = getFitter(probFitParams);
        predictedRevenueFitter = getFitter(predictedRevenueFitParams);
    }

    public double[] calculate(Integer percentile, Integer predictedRevenuePercentile) {

        double probFit = probabilityFitter.calculate(percentile);
        double revenueFit = predictedRevenueFitter.calculate(predictedRevenuePercentile);
        double expectedRevenueWithoutFitFunction = probFit * revenueFit;
        double[] result = new double[2];
        result[0] = expectedRevenueWithoutFitFunction;
        result[1] = probFit;
        return result;
    }

    private FittedConversionRateCalculator getFitter(FitFunctionParameters params) {
        switch (params.getVersion()) {
        case "v1":
            return new FittedConversionRateCalculatorImplV1(params);
        case "v2":
            return new FittedConversionRateCalculatorImplV2(params);
        default:
            throw new IllegalArgumentException("Unsupported fit function version " + params.getVersion());
        }
    }

    private EVFitFunctionParameters parseEVFitFunctionParams(String fitFunctionParamsStr) {
        return JsonUtils.deserialize(fitFunctionParamsStr, EVFitFunctionParameters.class);
    }
}
