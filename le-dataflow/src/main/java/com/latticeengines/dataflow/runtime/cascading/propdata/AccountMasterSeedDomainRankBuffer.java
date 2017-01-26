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
public class AccountMasterSeedDomainRankBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;

    private static final String ALEXA_RANK = "Rank";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private static final String FLAG_DROP_LESS_POPULAR_DOMAIN = "_FLAG_DROP_LESS_POPULAR_DOMAIN_";
    private static final String DOMAIN = "Domain";

    protected Map<String, Integer> namePositionMap;

    public AccountMasterSeedDomainRankBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        List<Tuple> tuples = new ArrayList<>();
        int minRank = Integer.MAX_VALUE;
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            tuples.add(arguments.getTupleCopy());
            int currentRank = arguments.getInteger(ALEXA_RANK);

            if (minRank > currentRank) {
                minRank = currentRank;
            }
        }

        if (tuples.size() == 1) {
            bufferCall.getOutputCollector().add(tuples.get(0));
        } else {
            String primaryDomain = "";
            for (Tuple tuple : tuples) {
                int currentRank = tuple.getInteger(namePositionMap.get(ALEXA_RANK));

                if (currentRank > minRank) {
                    continue;
                } else if (currentRank == minRank) {
                    primaryDomain = tuple.getString(namePositionMap.get(DOMAIN));
                }

                bufferCall.getOutputCollector().add(tuple);
            }

            for (Tuple tuple : tuples) {
                int currentRank = tuple.getInteger(namePositionMap.get(ALEXA_RANK));

                if (currentRank == minRank) {
                    continue;
                }

                tuple.setString(namePositionMap.get(LE_IS_PRIMARY_DOMAIN), "N");
                tuple.setString(namePositionMap.get(FLAG_DROP_LESS_POPULAR_DOMAIN), primaryDomain);
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
