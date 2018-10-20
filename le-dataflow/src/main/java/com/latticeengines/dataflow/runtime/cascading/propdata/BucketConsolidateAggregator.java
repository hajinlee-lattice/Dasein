package com.latticeengines.dataflow.runtime.cascading.propdata;

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

public class BucketConsolidateAggregator extends BaseAggregator<BucketConsolidateAggregator.Context>
        implements Aggregator<BucketConsolidateAggregator.Context> {

    private static final long serialVersionUID = -4558848041315363629L;
    private final String bktIdField;
    private final String bktCntField;
    private final List<String> grpByFields;
    private Integer bktIdArgPos;
    private Integer bktCntArgPos;
    // grpFields + bktsField + cntField
    public BucketConsolidateAggregator(List<String> grpByFields, String bktIdField,
            String bktCntField, String cntField, String bktsField) {
        super(generateFieldDeclaration(grpByFields, cntField, bktsField));
        this.grpByFields = grpByFields;
        this.bktIdField = bktIdField;
        this.bktCntField = bktCntField;
    }

    private static Fields generateFieldDeclaration(List<String> grpByFields, String cntField,
            String bktsField) {
        List<String> fields = new ArrayList<>(grpByFields);
        fields.add(cntField);
        fields.add(bktsField);
        return new Fields(fields.toArray(new String[fields.size()]));
    }

    static String serializeBktCnts(Map<Integer, Long> bktCnts) {
        List<String> tokens = new ArrayList<>();
        bktCnts.forEach((i, c) -> tokens.add(String.format("%d:%d", i, c)));
        return StringUtils.join(tokens, "|");
    }

    static Map<Integer, Long> deserializeBktCnts(String bktsStr) {
        String[] pairs = bktsStr.split("\\|");
        Map<Integer, Long> bktCnts = new HashMap<>();
        for (String pair : pairs) {
            String[] tokens = pair.split(":");
            Integer bktId = Integer.valueOf(tokens[0]);
            Long bktCnt = Long.valueOf(tokens[1]);
            bktCnts.put(bktId, bktCnt);
        }
        return bktCnts;
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
        int bktId = (int) arguments.getObject(bktIdArgPos);
        long bktCnt = (long) arguments.getObject(bktCntArgPos);
        context.bktCounts.put(bktId, bktCnt);
        if (bktId > 0) {
            context.count += bktCnt;
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
        if (bktIdArgPos == null) {
            int[] pos = arguments.getFields().getPos(new Fields(bktIdField, bktCntField));
            bktIdArgPos = pos[0];
            bktCntArgPos = pos[1];
        }
    }

    public static class Context extends BaseAggregator.Context {
        Map<Integer, Long> bktCounts = new HashMap<>();
        long count = 0L;
        Tuple result = new Tuple();
    }

}
