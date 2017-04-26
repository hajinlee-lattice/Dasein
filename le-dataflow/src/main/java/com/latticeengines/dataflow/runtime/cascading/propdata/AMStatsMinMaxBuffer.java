package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.StatsAttributeParser;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AMStatsMinMaxBuffer extends BaseOperation implements Buffer {
    private static final Log log = LogFactory.getLog(AMStatsMinMaxBuffer.class);

    private Map<String, Integer> namePositionMap;
    private String minMaxKey;

    public AMStatsMinMaxBuffer(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.minMaxKey = parameterObject.minMaxKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        StatsAttributeParser attributeParser = new StatsAttributeParser();
        ObjectMapper om = new ObjectMapper();

        Tuple result = Tuple.size(getFieldDeclaration().size());

        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        Map<String, Object[]> attributeMinMaxValues = new HashMap<>();
        Map<String, Object> groupKeyValues = new HashMap<>();

        calculateMinMax(attributeParser, arguments, attributeMinMaxValues, groupKeyValues);

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

    private void calculateMinMax(StatsAttributeParser attributeParser, //
            Iterator<TupleEntry> argumentsInGroup, //
            Map<String, Object[]> attributeMinMaxValues, //
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
                        attributeParser.parseNumericValForMinMax(//
                                attributeMinMaxValues, obj, fieldName);
                    }
                }
            }
            idx++;
        }
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
        String minMaxKey;
        Fields fieldDeclaration;

        public Params(Fields fieldDeclaration, String minMaxKey) {
            this.fieldDeclaration = fieldDeclaration;
            this.minMaxKey = minMaxKey;
        }
    }
}
