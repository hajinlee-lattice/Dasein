package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.EVFitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public class CalculateFittedExpectedRevenueFunction2 implements Serializable {

    private static final long serialVersionUID = -1608549821515170133L;

    public static final int EV_REVENUE_PRECISION = 2;
    public static final int PREDICTED_REVENUE_PRECISION = 6;
    private FittedConversionRateCalculator expectedRevenueFitter;
    private Double normalizationRatio;
    private Double minAllowedProbability;

    public CalculateFittedExpectedRevenueFunction2(Double normalizationRatio, Double avgProbabilityTestDataset,
            String evFitFunctionParamsStr) {

        this.normalizationRatio = normalizationRatio == null ? 1D : normalizationRatio;
        this.minAllowedProbability = avgProbabilityTestDataset == null ? 0D : (avgProbabilityTestDataset / 10D);

        EVFitFunctionParameters evFitFunctionParameters = parseEVFitFunctionParams(evFitFunctionParamsStr);
        expectedRevenueFitter = getFitter(evFitFunctionParameters.getEVParameters());

    }

    public double[] calculate(Integer percentile, double probability) {

        double[] result = new double[2];
        double fittedExpectedRevenue = expectedRevenueFitter.calculate(percentile);
        double normalizedExpectedRevenue = normalizationRatio == 0 ? 0 : fittedExpectedRevenue / normalizationRatio;
        double adjustedProbability = probability >= minAllowedProbability ? probability : minAllowedProbability;
        double adjustedPredictedRevenue = adjustedProbability == 0 ? 0
                : normalizedExpectedRevenue / adjustedProbability;
        try {
            adjustedPredictedRevenue = BigDecimal.valueOf(adjustedPredictedRevenue)
                    .setScale(PREDICTED_REVENUE_PRECISION, RoundingMode.HALF_UP).doubleValue();
        } catch (Exception ex) {
            throw new RuntimeException(String.format(
                    "Error: adjustedPredictedRevenue = %.4f, fittedExpectedRevenue = %.4f, normalizationRatio = %.4f, normalizedExpectedRevenue = %.4f, probability = %.4f, adjustedProbability = %.4f",
                    adjustedPredictedRevenue, fittedExpectedRevenue, normalizationRatio, normalizedExpectedRevenue,
                    probability, adjustedProbability), ex);
        }
        result[0] = adjustedProbability;
        normalizedExpectedRevenue = BigDecimal.valueOf(normalizedExpectedRevenue)
                .setScale(EV_REVENUE_PRECISION, RoundingMode.HALF_UP).doubleValue();
        result[1] = normalizedExpectedRevenue;
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
