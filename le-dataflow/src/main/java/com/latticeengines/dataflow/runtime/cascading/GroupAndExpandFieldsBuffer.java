package com.latticeengines.dataflow.runtime.cascading;

import java.util.Iterator;
import java.util.List;

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

    public GroupAndExpandFieldsBuffer(int numArgs, String expandField, List<String> expandFormats,
            Fields fieldDeclaration) {
        super(numArgs, fieldDeclaration);
        this.expandFieldName = expandField;
        this.expandFormas = expandFormats;
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
            int loc = getFieldDeclaration().getPos(fieldName);
            if (loc >= 0) {
                result.set(loc, group.getObject(fieldName));
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
            formatedFieldName = formatedFieldName.replaceAll("[ ,&]+", "");
            int loc = getFieldDeclaration().getPos(formatedFieldName);
            if (loc >= 0) {
                result.set(loc, tupleEntry.getObject(fieldName));
            }
        }
    }
}
