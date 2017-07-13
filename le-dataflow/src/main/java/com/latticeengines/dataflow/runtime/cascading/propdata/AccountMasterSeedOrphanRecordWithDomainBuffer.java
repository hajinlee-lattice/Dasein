package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "unused" })
public class AccountMasterSeedOrphanRecordWithDomainBuffer extends BaseOperation implements Buffer {
    private static final Logger log = LoggerFactory.getLogger(AccountMasterSeedOrphanRecordWithDomainBuffer.class);

    private static final long serialVersionUID = 4217950767704131475L;

    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String SALES_VOLUME_US_DOLLARS = "SALES_VOLUME_US_DOLLARS";
    private static final String FLAG_DROP_ORPHAN_ENTRY = "_FLAG_DROP_ORPHAN_ENTRY_";

    protected Map<String, Integer> namePositionMap;

    private int flagLoc;

    public AccountMasterSeedOrphanRecordWithDomainBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.flagLoc = this.namePositionMap.get(FLAG_DROP_ORPHAN_ENTRY);
    }

    // 1. keep all num loc > 0, not flag
    // 2. if none of them num loc > 0, keep one: use highest sales volume
    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();

        // initialize state variables
        boolean foundNonOrphan = false;
        TupleEntry highestSalesTuple = null;
        long highestSales = -1;

        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            if (isOrphan(arguments)) {
                if (foundNonOrphan) {
                    // if there is already loc > 0
                    // all orphan ones should be flagged
                    // no need for further check
                    Tuple tuple = flagTheTuple(arguments);
                    bufferCall.getOutputCollector().add(tuple);
                } else {
                    // still need to keep track highest sales, as there is
                    // possibility to keep one of the (loc==0) record
                    long thisSales = findSalesVolume(arguments);
                    if (thisSales > highestSales) {
                        if (highestSalesTuple != null) {
                            // release previous candidate
                            Tuple tuple = flagTheTuple(highestSalesTuple);
                            bufferCall.getOutputCollector().add(tuple);
                        }
                        // update state variables
                        highestSales = thisSales;
                        highestSalesTuple = new TupleEntry(arguments);
                    } else {
                        // it won't even be a candidate, flag and release
                        Tuple tuple = flagTheTuple(arguments);
                        bufferCall.getOutputCollector().add(tuple);
                    }
                }
            } else {
                // definitely return with no flag
                bufferCall.getOutputCollector().add(arguments);
                // update state variable
                foundNonOrphan = true;
            }
        }

        // handle the last tuple
        if (highestSalesTuple != null) {
            if (!foundNonOrphan) {
                bufferCall.getOutputCollector().add(highestSalesTuple);
            } else {
                // flag and release
                Tuple tuple = flagTheTuple(highestSalesTuple);
                bufferCall.getOutputCollector().add(tuple);
            }
        }

    }

    private boolean isOrphan(TupleEntry tupleEntry) {
        Object value = tupleEntry.getObject(LE_NUMBER_OF_LOCATIONS);
        return value != null && tupleEntry.getInteger(LE_NUMBER_OF_LOCATIONS) == 0;
    }

    private Long findSalesVolume(TupleEntry tupleEntry) {
        Object value = tupleEntry.getObject(SALES_VOLUME_US_DOLLARS);
        return value == null ? 0 : tupleEntry.getLong(SALES_VOLUME_US_DOLLARS);
    }

    private Tuple flagTheTuple(TupleEntry tupleEntry) {
        Tuple tuple = tupleEntry.getTupleCopy();
        tuple.setInteger(flagLoc, 1);
        return tuple;
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
