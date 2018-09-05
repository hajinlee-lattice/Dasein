package com.latticeengines.dataflow.runtime.cascading.cdl;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.EVFitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public class CalculateExpectedRevenueFunction extends BaseOperation implements Function {
    private String percentileFieldName;
    private String predictedRevenuePercentileFieldName;
    private int expectedRevenueFieldPos;
    private FittedConversionRateCalculator probabilityFitter;
    private FittedConversionRateCalculator predictedRevenueFitter;

    public CalculateExpectedRevenueFunction(Fields fieldDeclaration,
                                            String percentileFieldName,
                                            String predictedRevenuePercentileFieldName,
                                            String expectedRevenuePercentileFieldName,
                                            String evFitFunctionParamsStr) {
        super(fieldDeclaration);

        this.percentileFieldName = percentileFieldName;
        this.predictedRevenuePercentileFieldName = predictedRevenuePercentileFieldName;
        this.expectedRevenueFieldPos = fieldDeclaration.getPos(expectedRevenuePercentileFieldName);

        EVFitFunctionParameters evFitFunctionParameters = parseEVFitFunctionParams(evFitFunctionParamsStr);
        FitFunctionParameters probFitParams = evFitFunctionParameters.getProbabilityParameters();
        FitFunctionParameters predictedRevenueFitParams = evFitFunctionParameters.getRevenueParameters();
        probabilityFitter = getFitter(probFitParams);
        predictedRevenueFitter = getFitter(predictedRevenueFitParams);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry arguments = functionCall.getArguments();
        Integer percentile = arguments.getInteger(percentileFieldName);
        Integer predictedRevenuePercentile = arguments.getInteger(predictedRevenuePercentileFieldName);
        double probFit = probabilityFitter.calculate(percentile);
        double revenueFit = predictedRevenueFitter.calculate(predictedRevenuePercentile);
        double expectedRevenue = probFit * revenueFit;
        Tuple result = arguments.getTupleCopy();
        result.set(expectedRevenueFieldPos, expectedRevenue);

        functionCall.getOutputCollector().add(result);
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
