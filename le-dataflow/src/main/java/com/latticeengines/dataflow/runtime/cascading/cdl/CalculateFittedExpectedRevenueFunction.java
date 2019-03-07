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

    public static final int EV_REVENUE_PRECISION = 2;
    public static final int PREDICTED_REVENUE_PRECISION = 6;
    private String expectedRevenuePercentileFieldName;
    private String probabilityFieldName;
    private String predictedRevenueFieldName;
    private int expectedRevenueFieldPos;
    private int predictedRevenueFieldPos;
    private int backupPredictedRevFieldPos;
    private FittedConversionRateCalculator expectedRevenueFitter;

    public CalculateFittedExpectedRevenueFunction(Fields fieldDeclaration, String expectedRevenueFieldName,
            String expectedRevenuePercentileFieldName, String probabilityFieldName, String predictedRevenueFieldName,
            String backupPredictedRevFieldName, String evFitFunctionParamsStr) {
        super(fieldDeclaration);

        this.expectedRevenuePercentileFieldName = expectedRevenuePercentileFieldName;
        this.probabilityFieldName = probabilityFieldName;
        this.predictedRevenueFieldName = predictedRevenueFieldName;

        this.expectedRevenueFieldPos = fieldDeclaration.getPos(expectedRevenueFieldName);
        this.predictedRevenueFieldPos = fieldDeclaration.getPos(predictedRevenueFieldName);
        this.backupPredictedRevFieldPos = fieldDeclaration.getPos(backupPredictedRevFieldName);

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
        double probability = arguments.getDouble(probabilityFieldName);
        double originalPredictedRevenue = arguments.getDouble(predictedRevenueFieldName);
        Tuple result = arguments.getTupleCopy();
        double fittedExpectedRevenue = expectedRevenueFitter.calculate(percentile);
        double adjustedPredictedRevenue = fittedExpectedRevenue / probability;
        try {
            adjustedPredictedRevenue = BigDecimal.valueOf(adjustedPredictedRevenue)
                    .setScale(PREDICTED_REVENUE_PRECISION, RoundingMode.HALF_UP).doubleValue();
        } catch (Exception ex) {
            throw new RuntimeException(
                    String.format("Error: adjustedPredictedRevenue = %s, fittedExpectedRevenue = %s, probability = %s",
                            adjustedPredictedRevenue, fittedExpectedRevenue, probability, ex));
        }
        // copy the original predicted revenue into backupPredictedRevFieldPos
        // for backup and triaging any issue in future
        result.set(backupPredictedRevFieldPos, originalPredictedRevenue);
        // now overwrite adjusted predicted value into predictedRevenueFieldPos
        // (DSC-377)
        result.set(predictedRevenueFieldPos, adjustedPredictedRevenue);
        fittedExpectedRevenue = BigDecimal.valueOf(fittedExpectedRevenue)
                .setScale(EV_REVENUE_PRECISION, RoundingMode.HALF_UP).doubleValue();
        // now overwrite final fitted expected revenue value into
        // expectedRevenueFieldPos
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
