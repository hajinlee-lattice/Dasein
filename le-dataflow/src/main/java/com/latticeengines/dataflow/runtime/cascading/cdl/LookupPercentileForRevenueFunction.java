package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class LookupPercentileForRevenueFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(LookupPercentileForRevenueFunction.class);

    private static final long serialVersionUID = 8549465250465157539L;
    private int revenueFieldNamePos;
    private int revenuePercentileFieldNamePos;
    private List<BucketRange> percentiles;

    public LookupPercentileForRevenueFunction(Fields fieldDeclaration, String revenueFieldName,
            String revenuePercentileFieldName, ScoreDerivation revenueScoreDerivation) {
        super(fieldDeclaration);
        this.revenueFieldNamePos = fieldDeclaration.getPos(revenueFieldName);
        this.revenuePercentileFieldNamePos = fieldDeclaration.getPos(revenuePercentileFieldName);
        this.percentiles = revenueScoreDerivation.percentiles;
        log.info(String.format(
                "revenueFieldName = %s, revenuePercentileFieldName = %s,  revenueFieldNamePos = %d, "
                        + "revenuePercentileFieldNamePos = %d",
                revenueFieldName, revenuePercentileFieldName, revenueFieldNamePos, revenuePercentileFieldNamePos));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry arguments = functionCall.getArguments();
        Double revenue = (Double) arguments.getObject(revenueFieldNamePos, double.class);
        String percentileStr = percentiles.stream() //
                .parallel() //
                .filter(p -> p.lower <= revenue && revenue < p.upper) //
                .map(p -> {
                    return p.name;
                }) //
                .findFirst().orElse(null);

        if (StringUtils.isBlank(percentileStr)) {
            if (percentiles.get(percentiles.size() - 1).upper <= revenue) {
                percentileStr = percentiles.get(percentiles.size() - 1).name;
            } else {
                throw new RuntimeException();
            }
        }

        percentileStr = percentileStr.trim();
        Integer revenuePercentileBasedOnScoreDerivation = Integer.parseInt(percentileStr);

        Tuple result = arguments.getTupleCopy();
        result.set(revenuePercentileFieldNamePos, revenuePercentileBasedOnScoreDerivation);
        functionCall.getOutputCollector().add(result);
    }
}
