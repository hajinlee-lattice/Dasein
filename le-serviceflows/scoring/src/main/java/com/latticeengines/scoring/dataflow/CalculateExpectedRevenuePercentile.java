package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.dataflow.ev.CalculateFittedExpectedRevenueHelper;
import com.latticeengines.scoring.dataflow.ev.PercentileCalculationHelper;
import com.latticeengines.scoring.dataflow.ev.PercentileLookupAverageEvHelper;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;

import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

@Component("calculateExpectedRevenuePercentile")
public class CalculateExpectedRevenuePercentile
        extends TypesafeDataFlowBuilder<CalculateExpectedRevenuePercentileParameters> {
    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenuePercentile.class);

    private ParsedContext context;

    private static final String EV_PERCENTILE_EXPRESSION = "%s != null ? %s : %s";

    @Inject
    public Configuration yarnConfiguration;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    @Inject
    private PercentileLookupAverageEvHelper percentileLookupAverageEvHelper;

    @Inject
    public PercentileCalculationHelper percentileCalculationHelper;

    @Inject
    public CalculateFittedExpectedRevenueHelper calculateFittedExpectedRevenueHelper;

    @SuppressWarnings("deprecation")
    @Override
    public Node construct(CalculateExpectedRevenuePercentileParameters parameters) {
        log.info(String.format("%s = %s", parameters.getClass().getSimpleName(), JsonUtils.serialize(parameters)));
        context = new ParsedContext(parameters);

        Node inputTable = addSource(context.inputTableName);
        Node addPercentileColumn = inputTable.addColumnWithFixedValue(context.percentileFieldName, null, Integer.class);

        FieldList retainedFields = new FieldList(addPercentileColumn.getFieldNames());

        if (MapUtils.isNotEmpty(context.originalScoreFieldMap)) {
            // count number of rows
            Node mergedScoreCount = mergeCount(context, addPercentileColumn);

            // sort based on ExpectedRevenue column (and using number of
            // rows) calculate percentile and put value in new
            // ExpectedRevenuePercentile field
            Node calculatePercentile = percentileCalculationHelper.calculate(context, mergedScoreCount, false, false);

            calculatePercentile = calculatePercentile.retain(retainedFields);
            log.info(String.format("percentileFieldName '%s', standardScoreField '%s'", context.percentileFieldName,
                    context.standardScoreField));

            if (!context.standardScoreField.equals(context.percentileFieldName)) {
                retainedFields = new FieldList(calculatePercentile.getFieldNames());

                calculatePercentile = calculatePercentile //
                        .addFunction(
                                String.format(EV_PERCENTILE_EXPRESSION, context.percentileFieldName,
                                        context.percentileFieldName, context.standardScoreField), //
                                new FieldList(context.percentileFieldName, context.standardScoreField), //
                                new FieldMetadata(context.outputPercentileFieldName, Integer.class));

                // for each percentile bucket calculate average expected
                // revenue
                //
                // Use "ev" field from evScoreDerivation.json to lookup for
                // temporary EV percentile score for average expected revenue
                // value
                calculatePercentile = percentileLookupAverageEvHelper.calculate(context, calculatePercentile);

                // load evFitFunctionParamaters
                context.fitFunctionParametersMap = ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap(
                        context.customerSpace, yarnConfiguration, modelSummaryProxy, context.originalScoreFieldMap,
                        parameters.getFitFunctionParametersMap());

                log.info(String.format("fitFunctionParametersMap = %s",
                        JsonUtils.serialize(context.fitFunctionParametersMap)));

                // leaving the code for taking backup of PredictedRev and
                // Probability so that we can easily enable it in future. To
                // enable the backup just move next two lines between
                // declaration of Node inputTable and Node addPercentileColumn
                // and update Node references accordingly
                Node addBackupPredictedRevColumn = calculatePercentile
                        .addColumnWithFixedValue(context.backupPredictedRevFieldName, null, Double.class);
                Node addBackupProbabilityColumn = addBackupPredictedRevColumn
                        .addColumnWithFixedValue(context.backupProbabilityFieldName, null, Double.class);

                // initialize expectedRevenueFitter based on corresponding
                // fit function parameters
                //
                // for each row
                // calculate fitted expected revenue using this new
                // temporary EV percentile score and set it back to
                // ExpectedRevenue column
                //
                // copy values of ExpectedRevenuePercentile in original
                // percentile column ("Score") as downstream processing expects
                // final percentiles into original percentile column

                calculatePercentile = calculateFittedExpectedRevenue(retainedFields, addBackupProbabilityColumn);

                calculatePercentile = calculateFinalPercentile(retainedFields, calculatePercentile);

            }
            return calculatePercentile;
        }
        return addPercentileColumn;
    }

    @SuppressWarnings("deprecation")
    private Node calculateFinalPercentile(FieldList retainedFields, Node calculatePercentile) {
        Node mergedScoreCount;

        calculatePercentile = calculatePercentile.addColumnWithFixedValue(context.percentileFieldName, null,
                Integer.class);
        mergedScoreCount = mergeCount(context, calculatePercentile);
        calculatePercentile = percentileCalculationHelper.calculate(context, mergedScoreCount, true, context.targetScoreDerivation)
                .retain(retainedFields);

        calculatePercentile = calculatePercentile //
                .addFunction(
                        String.format(EV_PERCENTILE_EXPRESSION, context.percentileFieldName,
                                context.percentileFieldName, context.standardScoreField), //
                        new FieldList(context.percentileFieldName, context.standardScoreField), //
                        new FieldMetadata(ParsedContext.PREFIX_TEMP_COL + context.standardScoreField, Integer.class));
        calculatePercentile = calculatePercentile.discard(context.standardScoreField);
        calculatePercentile = calculatePercentile.rename(
                new FieldList(ParsedContext.PREFIX_TEMP_COL + context.standardScoreField),
                new FieldList(context.standardScoreField));
        return calculatePercentile;
    }

    @SuppressWarnings("deprecation")
    private Node calculateFittedExpectedRevenue(FieldList retainedFields, Node calculatePercentile) {
        String COPY_INERMEDIATE_EV = "%s";
        calculatePercentile = calculatePercentile //
                .addFunction(String.format(COPY_INERMEDIATE_EV, context.expectedRevenueField), //
                        new FieldList(context.expectedRevenueField), //
                        new FieldMetadata(context.outputExpRevFieldName, Double.class));

        List<String> tempRetainedFieldList = new ArrayList<>(retainedFields.getFieldsAsList());
        tempRetainedFieldList.add(context.outputExpRevFieldName);
        FieldList tempRetainedFields = new FieldList(tempRetainedFieldList);

        return calculateFittedExpectedRevenueHelper.calculate(context, calculatePercentile, tempRetainedFields);
    }

    private Node mergeCount(ParsedContext context, Node node) {
        Node score = node.retain(context.modelGuidFieldName)
                .renamePipe("modelScoreCount_" + System.currentTimeMillis());
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(context.modelGuidFieldName, String.class), //
                new FieldMetadata(context.scoreCountFieldName, Long.class) //
        );
        Node totalCount = score
                .groupByAndAggregate(new FieldList(context.modelGuidFieldName), //
                        new Count(new Fields(context.scoreCountFieldName)), //
                        scoreCountFms, Fields.ALL) //
                .retain(context.modelGuidFieldName, context.scoreCountFieldName);

        List<String> fieldsNames = new ArrayList<>(node.getFieldNames());
        fieldsNames.add(context.scoreCountFieldName);
        FieldList outputFields = new FieldList(fieldsNames);
        return node.innerJoin(context.modelGuidFieldName, totalCount, context.modelGuidFieldName).retain(outputFields);
    }

    public class ParsedContext {
        public static final String PREFIX_TEMP_COL = "__TEMP__";

        public final String standardScoreField = ScoreResultField.Percentile.displayName;
        public final String expectedRevenueField = ScoreResultField.ExpectedRevenue.displayName;
        public final String probabilityField = ScoreResultField.Probability.displayName;
        public final String predictedRevenueField = ScoreResultField.PredictedRevenue.displayName;

        public CustomerSpace customerSpace;
        public int minPct = 5;
        public int maxPct = 99;
        public String inputTableName;
        public String percentileFieldName;
        public String modelGuidFieldName;
        public Map<String, String> originalScoreFieldMap;
        public Map<String, String> fitFunctionParametersMap;
        public String outputPercentileFieldName;
        public String outputExpRevFieldName;
        public String backupPredictedRevFieldName;
        public String backupProbabilityFieldName;
        public String scoreCountFieldName;
        public boolean targetScoreDerivation;
        public Map<String, String> targetScoreDerivationPaths;

        public Map<String, Double> normalizationRatioMap;
        public Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps;

        ParsedContext(CalculateExpectedRevenuePercentileParameters parameters) {
            customerSpace = parameters.getCustomerSpace();
            inputTableName = parameters.getInputTableName();
            percentileFieldName = parameters.getPercentileFieldName();
            modelGuidFieldName = parameters.getModelGuidField();
            originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
            minPct = parameters.getPercentileLowerBound();
            maxPct = parameters.getPercentileUpperBound();
            normalizationRatioMap = parameters.getNormalizationRatioMap();
            scoreDerivationMaps = parameters.getScoreDerivationMaps();
            targetScoreDerivation = parameters.isTargetScoreDerivation();
            targetScoreDerivationPaths = parameters.getTargetScoreDerivationPaths();
            long timestamp = System.currentTimeMillis();
            outputPercentileFieldName = String.format("%sper_%d", PREFIX_TEMP_COL, timestamp);
            outputExpRevFieldName = String.format("%sev_%d", PREFIX_TEMP_COL, timestamp);
            scoreCountFieldName = String.format("%scount_%d", PREFIX_TEMP_COL, timestamp);
            backupPredictedRevFieldName = String.format("__%s_%s", predictedRevenueField, "raw");
            backupProbabilityFieldName = String.format("__%s_%s", probabilityField, "raw");
        }
    }
}
