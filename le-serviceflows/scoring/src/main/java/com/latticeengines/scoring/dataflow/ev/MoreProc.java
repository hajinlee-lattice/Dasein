package com.latticeengines.scoring.dataflow.ev;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

@Component("moreProc")
public class MoreProc {
    private static final Logger log = LoggerFactory.getLogger(MoreProc.class);

    private static final String EV_PERCENTILE_EXPRESSION = "%s != null ? %s : %s";

    @SuppressWarnings("deprecation")
    public Node additionalProcessing(ParsedContext context, Node calculatePercentile) {
        log.info(String.format("Using temporary ScoreField '%s' to merge values from %s and %s",
                context.outputPercentileFieldName, context.percentileFieldName, context.standardScoreField));

        log.info(String.format(
                "Using temporary evField '%s' to generate and store final ev using "
                        + "fitfunction and ev percentile field %s",
                context.outputExpRevFieldName, context.percentileFieldName));

        calculatePercentile = calculatePercentile //
                .addFunction(
                        String.format(EV_PERCENTILE_EXPRESSION, context.percentileFieldName,
                                context.percentileFieldName, context.standardScoreField), //
                        new FieldList(context.percentileFieldName, context.standardScoreField), //
                        new FieldMetadata(context.outputPercentileFieldName, Integer.class));
        return calculatePercentile;
    }
}
