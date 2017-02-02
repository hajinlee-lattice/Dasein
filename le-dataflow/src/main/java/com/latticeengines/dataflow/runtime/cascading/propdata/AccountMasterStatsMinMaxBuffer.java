package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AccountMasterStatsMinMaxBuffer extends BaseOperation implements Buffer {
    private static final Log log = LogFactory.getLog(AccountMasterStatsMinMaxBuffer.class);

    private Map<String, Integer> namePositionMap;
    private String minMaxKey;

    private ObjectMapper om = new ObjectMapper();

    public AccountMasterStatsMinMaxBuffer(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.minMaxKey = parameterObject.minMaxKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());

        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        Map<String, Object[]> attributeMinMaxValues = new HashMap<>();
        Map<String, Object> groupKeyValues = new HashMap<>();

        calculateMinMax(arguments, attributeMinMaxValues, groupKeyValues);

        try {
            result.setString(namePositionMap.get(minMaxKey), //
                    om.writeValueAsString(//
                            attributeMinMaxValues));
        } catch (JsonProcessingException e) {
            log.debug(e.getMessage(), e);
        }

        for (String key : groupKeyValues.keySet()) {
            result.set(namePositionMap.get(key), groupKeyValues.get(key));
        }

        bufferCall.getOutputCollector().add(result);
    }

    private void calculateMinMax(Iterator<TupleEntry> argumentsInGroup, //
            Map<String, Object[]> attributeManMaxValues, //
            Map<String, Object> groupKeyValues) {
        int idx = 0;
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Fields fields = arguments.getFields();

            int size = fields.size();

            for (int i = 0; i < size; i++) {
                if (arguments.getObject(i) != null) {
                    Object obj = arguments.getObject(i);
                    String fieldName = fields.get(i).toString();

                    if (namePositionMap.containsKey(fieldName)) {
                        if (idx == 0) {
                            groupKeyValues.put(fieldName, obj);
                        }
                        continue;
                    }

                    if (obj instanceof Long //
                            || obj instanceof Integer //
                            || obj instanceof Double) {
                        parseNumericValForMinMax(attributeManMaxValues, obj, fieldName);
                    }
                }
            }
            idx++;
        }
    }

    private void parseNumericValForMinMax(Map<String, Object[]> attributeManMaxValues, Object obj, String fieldName) {
        Object objVal = obj;
        if (!attributeManMaxValues.containsKey(fieldName)) {
            attributeManMaxValues.put(fieldName, null);
        }

        Object[] fieldBucketMap = attributeManMaxValues.get(fieldName);
        if (fieldBucketMap == null) {
            fieldBucketMap = new Object[2];
            fieldBucketMap[0] = objVal;
            fieldBucketMap[1] = objVal;
            attributeManMaxValues.put(fieldName, fieldBucketMap);
        }

        Object bucketMin = fieldBucketMap[0];
        Object bucketMax = fieldBucketMap[1];

        if (isGreater(bucketMin, objVal)) {
            fieldBucketMap[0] = objVal;
        } else if (isGreater(objVal, bucketMax)) {
            fieldBucketMap[1] = objVal;
        }
    }

    private boolean isGreater(Object firstVal, Object secondVal) {
        boolean isGreater = false;
        if (secondVal instanceof Long) {
            isGreater = ((Long) firstVal) > ((Long) secondVal);
        } else if (secondVal instanceof Integer) {
            isGreater = ((Integer) firstVal) > ((Integer) secondVal);
        } else if (secondVal instanceof Double) {
            isGreater = ((Double) firstVal) > ((Double) secondVal);
        }
        return isGreater;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    public static class Params {
        public String minMaxKey;
        public Fields fieldDeclaration;

        public Params(Fields fieldDeclaration, String minMaxKey) {
            this.fieldDeclaration = fieldDeclaration;
            this.minMaxKey = minMaxKey;
        }
    }
}
