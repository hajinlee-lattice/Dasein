package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
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
public class AccountMasterSeedOrphanRecordSmallCompaniesBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;

    private static final String FLAG_DROP_SMALL_BUSINESS = "_FLAG_DROP_SMALL_BUSINESS_";

    protected Map<String, Integer> namePositionMap;

    public AccountMasterSeedOrphanRecordSmallCompaniesBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        List<Tuple> tuples = new ArrayList<>();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            tuples.add(arguments.getTupleCopy());
        }

        if (tuples.size() == 1) {
            bufferCall.getOutputCollector().add(tuples.get(0));
        } else {
            Tuple mostPopulatedTuple = null;
            int maxNonNullFieldsSize = 0;
            for (Tuple tuple : tuples) {
                int fieldsSize = tuple.size();
                int nonNullFieldsSize = 0;
                for (int i = 0; i < fieldsSize; i++) {
                    if (tuple.getObject(i) != null) {
                        nonNullFieldsSize++;
                    }
                }

                if (maxNonNullFieldsSize < nonNullFieldsSize) {
                    maxNonNullFieldsSize = nonNullFieldsSize;
                    mostPopulatedTuple = tuple;
                }
            }
            bufferCall.getOutputCollector().add(mostPopulatedTuple);

            for (Tuple tuple : tuples) {
                if (tuple == mostPopulatedTuple) {
                    continue;
                }

                tuple.setInteger(namePositionMap.get(FLAG_DROP_SMALL_BUSINESS), 1);
                bufferCall.getOutputCollector().add(tuple);
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
