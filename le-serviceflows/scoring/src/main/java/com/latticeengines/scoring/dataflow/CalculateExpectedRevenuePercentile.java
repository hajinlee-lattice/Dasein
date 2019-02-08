package com.latticeengines.scoring.dataflow;

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
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.dataflow.ev.Helper;
import com.latticeengines.scoring.dataflow.ev.MoreMoreProc;
import com.latticeengines.scoring.dataflow.ev.MoreProc;
import com.latticeengines.scoring.dataflow.ev.MoreProc3;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;

@Component("calculateExpectedRevenuePercentile")
public class CalculateExpectedRevenuePercentile
        extends TypesafeDataFlowBuilder<CalculateExpectedRevenuePercentileParameters> {
    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenuePercentile.class);

    private ParsedContext context;

    @Inject
    public Configuration yarnConfiguration;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    @Inject
    private Helper helper;

    @Inject
    public MoreProc moreProc;

    @Inject
    public MoreMoreProc moreMoreProc;

    @Inject
    public MoreProc3 moreProc3;

    @Override
    public Node construct(CalculateExpectedRevenuePercentileParameters parameters) {
        log.info(String.format("%s = %s", parameters.getClass().getSimpleName(), JsonUtils.serialize(parameters)));
        context = new ParsedContext(parameters);

        Node inputTable = addSource(context.inputTableName);
        // 1. add new fixed column for ExpectedRevenuePercentile column
        Node addPercentileColumn = inputTable.addColumnWithFixedValue(context.percentileFieldName, null, Integer.class);
        FieldList retainedFields = new FieldList(addPercentileColumn.getFieldNames());

        if (MapUtils.isNotEmpty(context.originalScoreFieldMap)) {
            // 2. count number of rows
            Node mergedScoreCount = moreMoreProc.mergeCount(context, addPercentileColumn);

            // 3. sort based on ExpectedRevenue column (and using number of
            // rows) calculate percentile and put value in new
            // ExpectedRevenuePercentile field
            Node calculatePercentile = moreMoreProc.calculatePercentileByFieldMap(context, mergedScoreCount);

            calculatePercentile = calculatePercentile.retain(retainedFields);
            log.info(String.format("percentileFieldName '%s', standardScoreField '%s'", context.percentileFieldName,
                    context.standardScoreField));

            if (!context.standardScoreField.equals(context.percentileFieldName)) {
                retainedFields = new FieldList(calculatePercentile.getFieldNames());

                calculatePercentile = moreProc.additionalProcessing(context, calculatePercentile);

                // 4. for each percentile bucket calculate average expected
                // revenue
                //
                // 5. Use "ev" field from evScoreDerivation.json to lookup for
                // temporary EV percentile score for average expected revenue
                // value
                calculatePercentile = helper.doSomethingHere(context, calculatePercentile);

                // 6. load evFitFunctionParamaters
                context.fitFunctionParametersMap = ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap(
                        context.customerSpace, yarnConfiguration, modelSummaryProxy, context.originalScoreFieldMap,
                        parameters.getFitFunctionParametersMap());
                log.info(String.format("fitFunctionParametersMap = %s",
                        JsonUtils.serialize(context.fitFunctionParametersMap)));

                // 7. initialize expectedRevenueFitter based on corresponding
                // fit function parameters
                //
                // 8. for each row
                // 8.1. calculate fitted expected revenue using this new
                // temporary EV percentile score and set it back to
                // ExpectedRevenue column
                //
                // 9. copy values of ExpectedRevenuePercentile in original
                // percentile column ("Score") as downstream processing expects
                // final percentiles into original percentile column
                calculatePercentile = moreProc3.additionalProcessing2(context, calculatePercentile, retainedFields);
            }
            return calculatePercentile;
        }
        return addPercentileColumn;
    }

    public class ParsedContext {
        private static final String PREFIX_TEMP_COL = "__TEMP__";

        public final String standardScoreField = ScoreResultField.Percentile.displayName;
        public final String expectedRevenueField = ScoreResultField.ExpectedRevenue.displayName;

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
        public String scoreCountFieldName;

        ParsedContext(CalculateExpectedRevenuePercentileParameters parameters) {
            customerSpace = parameters.getCustomerSpace();
            inputTableName = parameters.getInputTableName();
            percentileFieldName = parameters.getPercentileFieldName();
            modelGuidFieldName = parameters.getModelGuidField();
            originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
            minPct = parameters.getPercentileLowerBound();
            maxPct = parameters.getPercentileUpperBound();
            long timestamp = System.currentTimeMillis();
            outputPercentileFieldName = String.format("%sper_%d", PREFIX_TEMP_COL, timestamp);
            outputExpRevFieldName = String.format("%sev_%d", PREFIX_TEMP_COL, timestamp);
            scoreCountFieldName = ScoreResultField.RawScore.displayName + "_count";
        }
    }
}
