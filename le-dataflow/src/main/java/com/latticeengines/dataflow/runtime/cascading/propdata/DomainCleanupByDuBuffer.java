package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomainCleanupByDuBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1L;
    private String duField;
    private String dunsField;
    private String domainField;

    public DomainCleanupByDuBuffer(List<String> fieldNames, String duField, String dunsField, String domainField) {
        super(new Fields(fieldNames.toArray(new String[0])));
        this.duField = duField;
        this.dunsField = dunsField;
        this.domainField = domainField;
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        TupleEntry group = bufferCall.getGroup();
        String groupValue = group.getString(0);
        if (StringUtils.isBlank(groupValue)) {
            returnTuplesAsIs(bufferCall, null);
            return;
        }

        List<TupleEntry> tuples = new ArrayList<>();
        List<String> mostUsedDomains = new ArrayList<>();
        Integer primaryDomainLoc = findDomainsAndLocations(bufferCall, tuples, mostUsedDomains);
        cleanupDomains(bufferCall, tuples, mostUsedDomains, primaryDomainLoc);

    }

    private void cleanupDomains(BufferCall bufferCall, List<TupleEntry> tuples, List<String> mostUsedDomains,
            Integer primaryDomainLoc) {
        String mostUsedDomain = mostUsedDomains.size() > 0 ? mostUsedDomains.get(0) : "";
        if ((primaryDomainLoc != null && StringUtils.isNotBlank(tuples.get(primaryDomainLoc).getString(domainField)))) {
            String primaryDomain = tuples.get(primaryDomainLoc).getString(domainField);
            fillBlankDomains(bufferCall, tuples, primaryDomain);
            return;
        } else {
            if (StringUtils.isNotBlank(mostUsedDomain)) {
                fillBlankDomains(bufferCall, tuples, mostUsedDomain);
                return;
            }
        }
        returnTuplesAsIs(bufferCall, tuples);
    }

    private void returnTuplesAsIs(BufferCall bufferCall, List<TupleEntry> tuples) {
        if (CollectionUtils.isNotEmpty(tuples)) {
            for (TupleEntry tuple : tuples) {
                bufferCall.getOutputCollector().add(tuple);
            }
        } else {
            @SuppressWarnings("unchecked")
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            while(iter.hasNext()) {
                bufferCall.getOutputCollector().add(iter.next());
            }
            
        }
    }

    private void fillBlankDomains(BufferCall bufferCall, List<TupleEntry> tuples, String enrichingDomain) {
        for (TupleEntry tuple : tuples) {
            String domain = tuple.getString(domainField);
            if (StringUtils.isBlank(domain)) {
                tuple.setString(domainField, enrichingDomain);
            }
            bufferCall.getOutputCollector().add(tuple);
        }
    }

    private Integer findDomainsAndLocations(BufferCall bufferCall, List<TupleEntry> tuples, List<String> mostUsedDomains) {
        int index = 0;
        Integer primaryDomainLoc = null;
        Map<String, Integer> domainCountMap = new HashMap<>();
        int maxCount = 0;
        String mostUsedDomainDomain = null;
        @SuppressWarnings("unchecked")
        Iterator<TupleEntry> argumentsIter = bufferCall.getArgumentsIterator();
        while (argumentsIter.hasNext()) {
            TupleEntry arguments = argumentsIter.next();
            String domain = arguments.getString(domainField);
            if (!StringUtils.isBlank(domain)) {
                if (domainCountMap.containsKey(domain)) {
                    domainCountMap.put(domain, domainCountMap.get(domain) + 1);
                } else {
                    domainCountMap.put(domain, 1);
                }
                if (domainCountMap.get(domain) > maxCount) {
                    maxCount = domainCountMap.get(domain);
                    mostUsedDomainDomain = domain;
                }
            }

            String duns = arguments.getString(dunsField);
            String du = arguments.getString(duField);
            if (StringUtils.isNotBlank(duns) && StringUtils.isNotBlank(du) && duns.equals(du)) {
                primaryDomainLoc = index;
            }
            tuples.add(new TupleEntry(arguments));
            index++;
        }
        if (mostUsedDomainDomain != null) {
            mostUsedDomains.add(mostUsedDomainDomain);
        }
        return primaryDomainLoc;
    }
}
