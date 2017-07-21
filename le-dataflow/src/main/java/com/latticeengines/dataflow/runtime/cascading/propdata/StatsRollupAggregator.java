package com.latticeengines.dataflow.runtime.cascading.propdata;

import static com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator.deserializeBktCnts;
import static com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator.serializeBktCnts;
import static com.latticeengines.dataflow.runtime.cascading.propdata.BucketExpandFunction.DIM_PREFIX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class StatsRollupAggregator extends BaseAggregator<StatsRollupAggregator.Context>
        implements Aggregator<StatsRollupAggregator.Context> {

    private static final long serialVersionUID = 1176540918011684429L;
    public static final String ALL = "__ALL__";

    public static class Context extends BaseAggregator.Context {
        Map<Integer, Long> bktCounts = new HashMap<>();
        long count = 0L;
        Tuple result = new Tuple();
    }

    private final String cntField;
    private final String bktsField;
    private final List<String> grpByFields;

    private Integer cntArgPos;
    private Integer bktsArgPos;
    private boolean needRollup;
    private boolean dedup; // when dedup, pick first instead of add up

    // grpFields + (rollupDims == __ALL__) + cntField + bktsField
    // if rollup is null, means simply merge bkt cnts
    public StatsRollupAggregator(List<String> grpByFields, String rollupDim, String cntField, String bktsField, boolean dedup) {
        super(generateFieldDeclaration(grpByFields, rollupDim, cntField, bktsField));
        this.grpByFields = grpByFields;
        this.cntField = cntField;
        this.bktsField = bktsField;
        this.needRollup = StringUtils.isNotBlank(rollupDim);
        this.dedup = dedup;
    }

    private static Fields generateFieldDeclaration(List<String> grpByFields, String rollupDim, String cntField, String bktsField) {
        List<String> fields = new ArrayList<>(grpByFields);
        if (StringUtils.isNotBlank(rollupDim)) {
            fields.add(DIM_PREFIX + rollupDim);
        }
        fields.add(cntField);
        fields.add(bktsField);
        return new Fields(fields.toArray(new String[fields.size()]));
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        for (String grpField : grpByFields) {
            context.result.add(group.getObject(grpField));
        }
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        updateArgPos(arguments);
        String bktsStr = (String) arguments.getObject(bktsArgPos);
        if (StringUtils.isNotBlank(bktsStr)) {
            Map<Integer, Long> bktCnts = deserializeBktCnts(bktsStr);
            bktCnts.forEach((bktId, bktCnt) -> {
                if (!context.bktCounts.containsKey(bktId)) {
                    context.bktCounts.put(bktId, bktCnt);
                } else if (!dedup) {
                    context.bktCounts.put(bktId, context.bktCounts.get(bktId) + bktCnt);
                }
            });
            if (!dedup) {
                long cnt = (long) arguments.getObject(cntArgPos);
                context.count += cnt;
            } else {
                context.count = 1L; // when dedup, all records with same dedup id only count as 1
            }
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        String serialized = serializeBktCnts(context.bktCounts);
        context.bktCounts.clear();
        Tuple result = context.result;
        if (needRollup) {
            result.add(ALL);
        }
        result.add(context.count);
        result.add(serialized);
        return result;
    }

    private void updateArgPos(TupleEntry arguments) {
        if (cntArgPos == null) {
            int[] pos = arguments.getFields().getPos(new Fields(cntField, bktsField));
            cntArgPos = pos[0];
            bktsArgPos = pos[1];
        }
    }

}
