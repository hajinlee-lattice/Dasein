package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

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
        String duns = bufferCall.getGroup().getString("DUNS");
        if (StringUtils.isBlank(duns)) {
            returnTuplesAsIs(bufferCall);
        } else {
            Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
            TupleEntry mostPopulatedTupleEntry = null;
            int maxNonNullFieldsSize = -1;
            while (argumentsInGroup.hasNext()) {
                TupleEntry arguments = argumentsInGroup.next();
                int nonNullFieldsSize = countNotNullFields(arguments);
                if (maxNonNullFieldsSize < nonNullFieldsSize) {
                    maxNonNullFieldsSize = nonNullFieldsSize;
                    // release previous candidate
                    if (mostPopulatedTupleEntry != null) {
                        bufferCall.getOutputCollector().add(mostPopulatedTupleEntry);
                    }
                    // update candidate
                    mostPopulatedTupleEntry = arguments.selectEntryCopy(Fields.ALL);
                } else {
                    // if not a candidate, directly output
                    bufferCall.getOutputCollector().add(arguments);
                }
            }
            if (mostPopulatedTupleEntry != null) {
                // change flag and release most populated tuple
                Tuple tuple = mostPopulatedTupleEntry.getTupleCopy();
                tuple.setInteger(namePositionMap.get(FLAG_DROP_SMALL_BUSINESS), 0);
                bufferCall.getOutputCollector().add(tuple);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void returnTuplesAsIs(BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            bufferCall.getOutputCollector().add(iter.next());
        }
    }

    private int countNotNullFields(TupleEntry tuple) {
        int fieldsSize = tuple.size();
        int nonNullFieldsSize = 0;
        for (int i = 0; i < fieldsSize; i++) {
            if (tuple.getObject(i) != null) {
                nonNullFieldsSize++;
            }
        }
        return nonNullFieldsSize;
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
