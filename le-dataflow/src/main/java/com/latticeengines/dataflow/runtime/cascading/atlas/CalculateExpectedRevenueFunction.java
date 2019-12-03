package com.latticeengines.dataflow.runtime.cascading.atlas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scoring.FittedConversionRateCalculator;
import com.latticeengines.domain.exposed.cdl.scoring.FittedConversionRateCalculatorImplV1;
import com.latticeengines.domain.exposed.cdl.scoring.FittedConversionRateCalculatorImplV2;
import com.latticeengines.domain.exposed.scoringapi.EVFitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CalculateExpectedRevenueFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenueFunction.class);

    private static final long serialVersionUID = 8549465250465151489L;
    private String percentileFieldName;
    private String predictedRevenuePercentileFieldName;
    private int expectedRevenueFieldPos;
    private int probabilityFieldPos;
    private FittedConversionRateCalculator probabilityFitter;
    private FittedConversionRateCalculator predictedRevenueFitter;

    public CalculateExpectedRevenueFunction(Fields fieldDeclaration, String percentileFieldName,
            String probabilityFieldName, String predictedRevenuePercentileFieldName, String expectedRevenueFieldName,
            String evFitFunctionParamsStr) {
        super(fieldDeclaration);

        this.percentileFieldName = percentileFieldName;
        this.predictedRevenuePercentileFieldName = predictedRevenuePercentileFieldName;
        this.expectedRevenueFieldPos = fieldDeclaration.getPos(expectedRevenueFieldName);
        this.probabilityFieldPos = fieldDeclaration.getPos(probabilityFieldName);

        EVFitFunctionParameters evFitFunctionParameters = parseEVFitFunctionParams(evFitFunctionParamsStr);
        FitFunctionParameters probFitParams = evFitFunctionParameters.getProbabilityParameters();
        FitFunctionParameters predictedRevenueFitParams = evFitFunctionParameters.getRevenueParameters();
        probabilityFitter = getFitter(probFitParams);
        predictedRevenueFitter = getFitter(predictedRevenueFitParams);
        log.info(String.format(
                "percentileFieldName = %s, predictedRevenuePercentileFieldName = %s,  expectedRevenuePercentileFieldName = %s, "
                        + "expectedRevenueFieldPos = %d",
                percentileFieldName, predictedRevenuePercentileFieldName, expectedRevenueFieldName,
                expectedRevenueFieldPos));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry arguments = functionCall.getArguments();
        Integer percentile = arguments.getInteger(percentileFieldName);
        Integer predictedRevenuePercentile = arguments.getInteger(predictedRevenuePercentileFieldName);
        double probFit = probabilityFitter.calculate(percentile);
        double revenueFit = predictedRevenueFitter.calculate(predictedRevenuePercentile);
        double expectedRevenueWithoutFitFunction = probFit * revenueFit;
        Tuple result = arguments.getTupleCopy();
        result.set(expectedRevenueFieldPos, expectedRevenueWithoutFitFunction);
        result.set(probabilityFieldPos, probFit);
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
