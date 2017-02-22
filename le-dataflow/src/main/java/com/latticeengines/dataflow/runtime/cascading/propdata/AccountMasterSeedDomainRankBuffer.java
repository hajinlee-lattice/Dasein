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
public class AccountMasterSeedDomainRankBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;

    public static final String MIN_RANK_DOMAIN = "_MinAlexaRankDomain_";

    private static final String ALEXA_RANK = "Rank";
    private static final String DOMAIN = "Domain";
    private static final String DUNS = "DUNS";

    protected Map<String, Integer> namePositionMap;

    // output (DUNS, Domain)
    public AccountMasterSeedDomainRankBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        String duns = bufferCall.getGroup().getString(DUNS);
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        if (!StringUtils.isBlank(duns)) {
            String minRankDomain = minRankDomainForDuns(argumentsInGroup);
            Tuple result = Tuple.size(2);
            result.set(0, duns);
            result.set(1, minRankDomain);
            bufferCall.getOutputCollector().add(result);
        }
    }

    private String minRankDomainForDuns(Iterator<TupleEntry> argumentsInGroup) {
        int minRank = Integer.MAX_VALUE;
        String minRankDomain = null;
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            int currentRank = arguments.getInteger(ALEXA_RANK);
            String currentDomain = arguments.getString(DOMAIN);
            if (minRank > currentRank) {
                minRank = currentRank;
                minRankDomain = currentDomain;
            }
        }
        return minRankDomain;
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
