package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

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

    public AccountMasterSeedOrphanRecordWithDomainBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry groupValues = bufferCall.getGroup();
        if (groupValuesHasNull(groupValues)) {
            // if any of the group by value is null or blank
            // return all tuples as is
            returnTuplesAsIs(bufferCall);
        } else {
            Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
            List<Tuple> tuples = new ArrayList<>();
            while (argumentsInGroup.hasNext()) {
                TupleEntry arguments = argumentsInGroup.next();
                tuples.add(arguments.getTupleCopy());
            }

            if (tuples.size() == 1) {
                bufferCall.getOutputCollector().add(tuples.get(0));
            } else {
                for (Tuple tuple : tuples) {
                    Integer currentNumberOfLocations = tuple.getInteger(namePositionMap.get(LE_NUMBER_OF_LOCATIONS));

                    if (currentNumberOfLocations != null && currentNumberOfLocations.intValue() <= 0) {
                        tuple.setInteger(namePositionMap.get(FLAG_DROP_ORPHAN_ENTRY), 1);
                        bufferCall.getOutputCollector().add(tuple);
                        continue;
                    }
                    bufferCall.getOutputCollector().add(tuple);
                }
            }
        }
    }

    private boolean groupValuesHasNull(TupleEntry groupValues) {
        for (Object value: groupValues.asIterableOf(Object.class)) {
            if (value == null) {
                return true;
            }
            String strValue = null;
            if (value instanceof Utf8) {
                strValue = value.toString();
            }
            if (value instanceof String) {
                strValue = (String) value;
            }
            if (StringUtils.isBlank(strValue)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void returnTuplesAsIs(BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            bufferCall.getOutputCollector().add(iter.next());
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
