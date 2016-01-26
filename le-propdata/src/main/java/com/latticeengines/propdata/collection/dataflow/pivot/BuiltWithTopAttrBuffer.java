package com.latticeengines.propdata.collection.dataflow.pivot;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import liquibase.util.StringUtils;

@SuppressWarnings("rawtypes")
public class BuiltWithTopAttrBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -570985038993978462L;

    private static final String TECHNOLOGY_NAME = "Technology_Name";
    private static final String TECHNOLOGY_FIRST = "Technology_First_Detected";
    private static final int NUM_ATTR = 6;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

    protected Map<String, Integer> namePositionMap;
    private Map<String, String> attrMap;

    private BuiltWithTopAttrBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    protected BuiltWithTopAttrBuffer(Map<String, String> attrMap, Fields fieldDeclaration) {
        this(fieldDeclaration);
        this.attrMap = attrMap;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field: fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(result, arguments);
        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field: fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        Map<Integer, Set<String>> countContextMap = new HashMap<>();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String tech = arguments.getString(TECHNOLOGY_NAME);
            Long firstDetected = arguments.getLong(TECHNOLOGY_FIRST);
            if (attrMap.containsKey(tech)) {
                String resultColumn = attrMap.get(tech);
                Integer loc = namePositionMap.get(resultColumn);
                String token = tech + "(" + dateFormat.format(new Date(firstDetected)) + ")";
                if (countContextMap.containsKey(loc) && countContextMap.get(loc).size() < NUM_ATTR) {
                    countContextMap.get(loc).add(token);
                } else {
                    countContextMap.put(loc, new HashSet<>(Collections.singleton(token)));
                }
            }
        }
        for (Map.Entry<Integer, Set<String>> entry: countContextMap.entrySet()) {
            Integer loc = entry.getKey();
            if (loc >= 0) {
                result.set(loc, StringUtils.join(entry.getValue(), ","));
            }
        }
    }


}
