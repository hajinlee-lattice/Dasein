package com.latticeengines.dataflow.runtime.cascading.propdata;

import static com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator.deserializeBktCnts;
import static com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator.serializeBktCnts;

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

public class StatsDedupAggregator extends BaseAggregator<StatsDedupAggregator.Context>
        implements Aggregator<StatsDedupAggregator.Context> {

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

    // grpFields + cntField + bktsField
    // if rollup is null, means simply merge bkt cnts
    public StatsDedupAggregator(List<String> grpByFields, String dedupField, String cntField, String bktsField) {
        super(generateFieldDeclaration(grpByFields, cntField, bktsField));
        this.grpByFields = grpByFields;
        this.cntField = cntField;
        this.bktsField = bktsField;
    }

    private static Fields generateFieldDeclaration(List<String> grpByFields, String cntField, String bktsField) {
        List<String> fields = new ArrayList<>(grpByFields);
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
                    // only put when there is not count
                    context.bktCounts.put(bktId, bktCnt);
                }
            });
            long cnt = (long) arguments.getObject(cntArgPos);
            context.count += cnt;
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        String serialized = serializeBktCnts(context.bktCounts);
        context.bktCounts.clear();
        Tuple result = context.result;
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
