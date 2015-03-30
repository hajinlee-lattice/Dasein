package com.latticeengines.dataflow.runtime.cascading;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class GroupAndExpandFieldsBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1505662991286452847L;
    private String expandFieldName;
    private List<String> expandFormas;
    private Map<String, Integer> namePositionMap;

    public GroupAndExpandFieldsBuffer(int numArgs, String expandField, List<String> expandFormats,
            Fields fieldDeclaration) {
        super(numArgs, fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.expandFieldName = expandField;
        this.expandFormas = expandFormats;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        Iterator iterator = fieldDeclaration.iterator();
        int pos = 0;
        while (iterator.hasNext()) {
            String fieldName = (String) iterator.next();
            positionMap.put(fieldName.toLowerCase(), pos++);
        }
        return positionMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);

        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        while (arguments.hasNext()) {
            TupleEntry argument = arguments.next();
            setupTupleForArgument(result, argument);
        }

        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        Iterator iterator = fields.iterator();
        while (iterator.hasNext()) {
            String fieldName = (String) iterator.next();
            if (fieldName.equals(expandFieldName)) {
                continue;
            }
            Integer loc = namePositionMap.get(fieldName.toLowerCase());
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, TupleEntry tupleEntry) {

        Fields fields = tupleEntry.getFields();
        String expandFieldValue = "" + tupleEntry.getObject(expandFieldName);
        int i = 0;
        Iterator iterator = fields.iterator();
        while (iterator.hasNext()) {
            String fieldName = (String) iterator.next();
            if (fieldName.equals(expandFieldName)) {
                continue;
            }
            String format = expandFormas.get(i++);
            String formatedFieldName = String.format(format, expandFieldValue);
            formatedFieldName = formatedFieldName.replaceAll("[ ,&/()]+", "");
            Integer loc = namePositionMap.get(formatedFieldName.toLowerCase());
            if (loc != null && loc >= 0) {
                result.set(loc, tupleEntry.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }
}
