package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.EVFitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.EVScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CalculatePositiveEventsFunction extends BaseOperation
        implements Function, FittedConversionRateCalculatorFactory {

    private static final long serialVersionUID = -3505533055111984029L;
    private String avgScoreFieldName;
    private String totalEventFieldName;
    private RawScoreToPercentileMapper rawScoreToPercentileMapper;
    private FittedConversionRateCalculator fittedConversionRateCalculator;

    public CalculatePositiveEventsFunction(String totalPositiveEventsFieldName, String avgScoreFieldName,
            String totalEventFieldName, String scoreDerivationStr, String fitFunctionParamsStr, boolean isEV) {
        super(new Fields(totalPositiveEventsFieldName));
        this.avgScoreFieldName = avgScoreFieldName;
        this.totalEventFieldName = totalEventFieldName;
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
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Double avgRawScore = arguments.getDouble(avgScoreFieldName);
        long totalEvent = arguments.getLong(totalEventFieldName);
        int mappedPercentile = rawScoreToPercentileMapper.map(avgRawScore);
        double conversionRate = fittedConversionRateCalculator.calculate(mappedPercentile);
        double positiveEvents = conversionRate * totalEvent;
        Tuple result = Tuple.size(1);
        result.setDouble(0, positiveEvents);
        functionCall.getOutputCollector().add(result);
    }

    @Override
    public FittedConversionRateCalculator getCalculator(FitFunctionParameters params) {
        switch (params.getVersion()) {
            case "v1":
                return new FittedConversionRateCalculatorImplV1(params);
            case "v2":
                return new FittedConversionRateCalculatorImplV2(params);
            default:
                throw new IllegalArgumentException(
                        "Unsupported fit function version " + params.getVersion());
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
