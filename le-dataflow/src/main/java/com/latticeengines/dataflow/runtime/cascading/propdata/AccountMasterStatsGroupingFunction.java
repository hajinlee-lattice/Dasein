package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterStatsGroupingFunction extends BaseOperation implements Buffer {
    private static final Log log = LogFactory.getLog(AccountMasterStatsGroupingFunction.class);

    protected Map<String, Integer> namePositionMap;

    private int maxAttrs;

    private Integer totalLoc;

    private Map<Comparable, Integer> attrIdMap;

    private String[] dimensionIdFieldNames;

    public AccountMasterStatsGroupingFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        maxAttrs = parameterObject.attrFields.length;
        namePositionMap = new HashMap<>();
        dimensionIdFieldNames = parameterObject.dimensionIdFieldNames;

        for (int i = 0; i < parameterObject.attrFields.length; i++) {
            namePositionMap.put(parameterObject.attrs[i], parameterObject.attrIds[i]);
        }
        namePositionMap.put(parameterObject.totalField, parameterObject.attrs.length);
        this.totalLoc = namePositionMap.get(parameterObject.totalField);

        attrIdMap = new HashMap<Comparable, Integer>();

        for (int i = 0; i < parameterObject.attrs.length; i++) {
            Integer attrId = parameterObject.attrIds[i];
            if (attrId >= maxAttrs) {
                log.info("Skip attribute " + parameterObject.attrs[i] + " for invalid id " + attrId);
                continue;
            }
            attrIdMap.put(parameterObject.attrs[i], attrId);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());

        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        long[] attrCount = new long[maxAttrs];

        long totalCount = countAttrsForArgument(attrCount, arguments);

        for (int i = 0; i < attrCount.length - dimensionIdFieldNames.length; i++) {
            result.setLong(i, attrCount[i]);
        }

        result.setLong(totalLoc, totalCount);
        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                log.error("Can not find field name=" + fieldName);
            }
        }
    }

    private long countAttrsForArgument(long[] attrCount, Iterator<TupleEntry> argumentsInGroup) {
        int totalCount = 0;
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Fields fields = arguments.getFields();
            int size = fields.size();

            for (int i = 0; i < size; i++) {
                if (arguments.getObject(i) != null) {
                    Integer attrId = attrIdMap.get(fields.get(i));
                    if (attrId != null) {
                        attrCount[attrId]++;
                    }
                }
            }
            totalCount += 1;
        }
        return totalCount;
    }

    public static class Params {
        public String[] attrs;
        public Integer[] attrIds;
        public Fields fieldDeclaration;
        public String[] attrFields;
        public String totalField;
        public String[] dimensionIdFieldNames;

        public Params(String[] attrs, Integer[] attrIds, //
                Fields fieldDeclaration, String[] attrFields, String totalField, String[] dimensionIdFieldNames) {
            this.attrs = attrs;
            this.attrIds = attrIds;
            this.fieldDeclaration = fieldDeclaration;
            this.attrFields = attrFields;
            this.totalField = totalField;
            this.dimensionIdFieldNames = dimensionIdFieldNames;
        }
    }
}
