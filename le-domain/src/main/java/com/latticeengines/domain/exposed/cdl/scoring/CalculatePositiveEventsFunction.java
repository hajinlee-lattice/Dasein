package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.EVFitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.EVScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class CalculatePositiveEventsFunction implements FittedConversionRateCalculatorFactory, Serializable {

    private static final long serialVersionUID = 2659841538770732506L;
    private RawScoreToPercentileMapper rawScoreToPercentileMapper;
    private FittedConversionRateCalculator fittedConversionRateCalculator;

    public CalculatePositiveEventsFunction(String scoreDerivationStr, String fitFunctionParamsStr, boolean isEV) {
        ScoreDerivation scoreDerivation;
        FitFunctionParameters fitFunctionParameters;
        if (isEV) {
            scoreDerivation = parseEVScoreDerivation(scoreDerivationStr).getEVScoreDerivation();
            fitFunctionParameters = parseEVFitFunctionParams(fitFunctionParamsStr).getEVParameters();
        } else {
            scoreDerivation = parseScoreDerivation(scoreDerivationStr);
            fitFunctionParameters = parseFitFunctionParams(fitFunctionParamsStr);
        }
        this.rawScoreToPercentileMapper = new RawScoreToPercentileMapper(scoreDerivation);
        this.fittedConversionRateCalculator = getCalculator(fitFunctionParameters);
    }

    @Override
    public double calculate(Double avgRawScore, long totalEvent) {
        int mappedPercentile = rawScoreToPercentileMapper.map(avgRawScore);
        double conversionRate = fittedConversionRateCalculator.calculate(mappedPercentile);
        double positiveEvents = conversionRate * totalEvent;
        return positiveEvents;
    }

    @Override
    public FittedConversionRateCalculator getCalculator(FitFunctionParameters params) {
        switch (params.getVersion()) {
        case "v1":
            return new FittedConversionRateCalculatorImplV1(params);
        case "v2":
            return new FittedConversionRateCalculatorImplV2(params);
        default:
            throw new IllegalArgumentException("Unsupported fit function version " + params.getVersion());
        }
    }

    private FitFunctionParameters parseFitFunctionParams(String fitFunctionParamsStr) {
        return JsonUtils.deserialize(fitFunctionParamsStr, FitFunctionParameters.class);
    }

    private ScoreDerivation parseScoreDerivation(String scoreDerivationStr) {
        return JsonUtils.deserialize(scoreDerivationStr, ScoreDerivation.class);
    }

    private EVFitFunctionParameters parseEVFitFunctionParams(String fitFunctionParamsStr) {
        return JsonUtils.deserialize(fitFunctionParamsStr, EVFitFunctionParameters.class);
    }

    private EVScoreDerivation parseEVScoreDerivation(String scoreDerivationStr) {
        return JsonUtils.deserialize(scoreDerivationStr, EVScoreDerivation.class);
    }

}
