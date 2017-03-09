package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttributeStatsDetailsMergeUtil;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterStatsDimensionAggregator
        extends BaseAggregator<AccountMasterStatsDimensionAggregator.Context>
        implements Aggregator<AccountMasterStatsDimensionAggregator.Context> {
    private static final long serialVersionUID = 4217950767704131475L;
    private static ObjectMapper OM = new ObjectMapper();

    public static class Context extends BaseAggregator.Context {
        Tuple mergedTuple  = null;
        int numOfFields;
        int recordCnt = 0;
    }


    public AccountMasterStatsDimensionAggregator(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {

        if (context.mergedTuple == null) {
            context.mergedTuple = arguments.getTupleCopy();
            context.numOfFields = context.mergedTuple.size();
        } else {
            context.mergedTuple = merge(context.mergedTuple, arguments.getTuple(), context.numOfFields);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple tuple = context.mergedTuple;
        context.mergedTuple = null;
        return tuple;
    }

    private Tuple merge(Tuple existingMergedTuple, Tuple originalTuple, int numOfFields) {
        AttributeStatsDetails mergedStats = null;
        for (int idx = 0; idx < numOfFields; idx++) {
            Object objInExistingMergedTuple = existingMergedTuple.getObject(idx);
            Object objInOriginalTuple = originalTuple.getObject(idx);
            boolean isPrint = false;

            if (objInExistingMergedTuple == null) {
                existingMergedTuple.set(idx, objInOriginalTuple);
            } else if (objInOriginalTuple == null) {
                existingMergedTuple.set(idx, objInExistingMergedTuple);
            } else if (objInExistingMergedTuple instanceof String) {
                AttributeStatsDetails statsInExistingMergedTuple = null;
                AttributeStatsDetails statsInOriginalTuple = null;

                try {
                    statsInExistingMergedTuple = OM.readValue((String) objInExistingMergedTuple,
                            AttributeStatsDetails.class);
                    statsInOriginalTuple = OM.readValue((String) objInOriginalTuple, AttributeStatsDetails.class);
                } catch (IOException e) {
                    // ignore if type of serialized obj is not
                    // statsInExistingMergedTuple
                    System.out.println("Something wrong");
                    continue;
                }

                mergedStats = merge(statsInExistingMergedTuple, statsInOriginalTuple, isPrint);
                try {
                    existingMergedTuple.set(idx, OM.writeValueAsString(mergedStats));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return existingMergedTuple;
    }

    private AttributeStatsDetails merge(AttributeStatsDetails statsInExistingMergedTuple,
            AttributeStatsDetails statsInOriginalTuple, boolean isPrint) {
        AttributeStatsDetails mergedAttrStats = AttributeStatsDetailsMergeUtil
                .addStatsDetails(statsInExistingMergedTuple, statsInOriginalTuple, isPrint);
        return mergedAttrStats;
    }
}
