package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
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
public class CalculateFittedExpectedRevenueFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenueFunction.class);

    private static final long serialVersionUID = 8540065221465151489L;

    private static final int EV_REVENUE_PRECISION = 2;
    private String expectedRevenuePercentileFieldName;
    private int expectedRevenueFieldPos;
    private FittedConversionRateCalculator expectedRevenueFitter;

    public CalculateFittedExpectedRevenueFunction(Fields fieldDeclaration, String expectedRevenueFieldName,
            String expectedRevenuePercentileFieldName, String evFitFunctionParamsStr) {
        super(fieldDeclaration);

        this.expectedRevenuePercentileFieldName = expectedRevenuePercentileFieldName;
        this.expectedRevenueFieldPos = fieldDeclaration.getPos(expectedRevenueFieldName);

        EVFitFunctionParameters evFitFunctionParameters = parseEVFitFunctionParams(evFitFunctionParamsStr);
        expectedRevenueFitter = getFitter(evFitFunctionParameters.getEVParameters());
        log.info(String.format(
                "expectedRevenuePercentileFieldName = %s, expectedRevenueFieldName = %s, "
                        + "expectedRevenueFieldPos = %d",
                expectedRevenuePercentileFieldName, expectedRevenueFieldName, expectedRevenueFieldPos));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry arguments = functionCall.getArguments();
        Integer percentile = arguments.getInteger(expectedRevenuePercentileFieldName);
        double fittedExpectedRevenue = expectedRevenueFitter.calculate(percentile);
        fittedExpectedRevenue = BigDecimal.valueOf(fittedExpectedRevenue)
                .setScale(EV_REVENUE_PRECISION, RoundingMode.HALF_UP).doubleValue();

        Tuple result = arguments.getTupleCopy();
        result.set(expectedRevenueFieldPos, fittedExpectedRevenue);

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
