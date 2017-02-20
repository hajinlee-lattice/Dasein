package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({"rawtypes", "unused"})
public class AccountMasterSeedOrphanRecordWithDomainBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;

    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String FLAG_DROP_ORPHAN_ENTRY = "_FLAG_DROP_ORPHAN_ENTRY_";

    protected Map<String, Integer> namePositionMap;
    private int domainLoc;
    private int latticeIdLoc;

    // TODO: this buffer probably need to change: keep at lease one non-orphan for each DUNS
    // if not, the current impl can be changed to a function
    public AccountMasterSeedOrphanRecordWithDomainBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Object value = arguments.getObject(LE_NUMBER_OF_LOCATIONS);
            if (value != null) {
                int currentNumberOfLocations = arguments.getInteger(LE_NUMBER_OF_LOCATIONS);
                if (currentNumberOfLocations == 0) {
                    bufferCall.getOutputCollector().add(arguments);
                } else {
                    Tuple tuple = arguments.getTupleCopy();
                    tuple.setInteger(namePositionMap.get(FLAG_DROP_ORPHAN_ENTRY), 1);
                    bufferCall.getOutputCollector().add(tuple);
                }
            } else {
                bufferCall.getOutputCollector().add(arguments);
            }
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
}
