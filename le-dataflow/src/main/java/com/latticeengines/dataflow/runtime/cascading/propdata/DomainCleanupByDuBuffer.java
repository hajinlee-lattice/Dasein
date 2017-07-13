package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomainCleanupByDuBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(DomainCleanupByDuBuffer.class);
    public static final String DU_PRIMARY_DOMAIN = "DUPrimaryDomain";

    private String duField;
    private String dunsField;
    private String domainField;
    private String alexaRankField;
    protected Map<String, Integer> namePositionMap;

    private int duArgIdx = -1;
    private int dunsArgIdx = -1;
    private int domainArgIdx = -1;
    private int alexaRankArgIdx = -1;

    // output (DU, PrimaryDomain)
    public DomainCleanupByDuBuffer(Fields fieldDeclaration, String duField, String dunsField, String domainField,
            String alexaRankField) {
        super(fieldDeclaration);
        this.duField = duField;
        this.dunsField = dunsField;
        this.domainField = domainField;
        this.alexaRankField = alexaRankField;
        namePositionMap = getPositionMap(fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry group = bufferCall.getGroup();
        String groupValue = group.getString(0);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        if (StringUtils.isNotBlank(groupValue)) {
            // only process groups with DU != null
            Integer duLoc = namePositionMap.get(duField.toLowerCase());
            result.set(duLoc, groupValue);

            String primaryOrMostUsedDomain = findPrimaryOrMostUsedDomain(bufferCall);
            result = outputPrimaryDomain(result, primaryOrMostUsedDomain);
        }
        bufferCall.getOutputCollector().add(result);
    }

    private Tuple outputPrimaryDomain(Tuple result, String primaryOrMostRecentDomain) {
        Integer pdLoc = namePositionMap.get(DU_PRIMARY_DOMAIN.toLowerCase());
        if (StringUtils.isNotBlank(primaryOrMostRecentDomain)) {
            // we have most used domain, but that tuple does not satisfy DUNS=DU
            result.set(pdLoc, primaryOrMostRecentDomain);
            return result;
        } else {
            result.set(pdLoc, null);
            return result;
        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName.toLowerCase(), pos++);
        }
        return positionMap;
    }

    @SuppressWarnings("unchecked")
    private String findPrimaryOrMostUsedDomain(BufferCall bufferCall) {
        String mostUsedDomain = null;
        String primaryDomain = null;
        Map<String, Integer> domainCountMap = new HashMap<>();
        int maxCount = 0;
        int numTuples = 0;
        String du = null;
        Iterator<TupleEntry> argumentsIter = bufferCall.getArgumentsIterator();
        String maxAlexaRankDomain = null;
        Integer maxAlexaRank = Integer.MAX_VALUE;
        while (argumentsIter.hasNext()) {
            numTuples++;
            TupleEntry arguments = argumentsIter.next();
            setArgPosMap(arguments);
            String domain = getStringAt(arguments, domainArgIdx);
            if (StringUtils.isNotBlank(domain)) {
                if (domainCountMap.containsKey(domain)) {
                    domainCountMap.put(domain, domainCountMap.get(domain) + 1);
                } else {
                    domainCountMap.put(domain, 1);
                }
                if (domainCountMap.get(domain) > maxCount) {
                    maxCount = domainCountMap.get(domain);
                    mostUsedDomain = domain;
                }
            }

            String duns = getStringAt(arguments, dunsArgIdx);
            du = getStringAt(arguments, duArgIdx);
            if (StringUtils.isNotBlank(duns) && StringUtils.isNotBlank(du) && duns.equals(du)) {
                String domainInTuple = getStringAt(arguments, domainArgIdx);
                if (StringUtils.isNotBlank(domainInTuple)) {
                    primaryDomain = domainInTuple;
                    break;
                }
            }

            Object alexaRank = arguments.getObject(alexaRankArgIdx);
            if (alexaRank != null && StringUtils.isNotBlank(alexaRank.toString())) {
                Integer alexRankInt = Integer.valueOf(alexaRank.toString());
                if (alexRankInt < maxAlexaRank && StringUtils.isNotBlank(domain)) {
                    maxAlexaRank = alexRankInt;
                    maxAlexaRankDomain = domain;
                }
            }
        }
        if (numTuples >= 100000) {
            if (primaryDomain == null) {
                log.warn("The group of DU=" + du + " has " + numTuples + " tuples. No primary domain.");
            } else {
                log.warn("Found a primary domain for the group of DU=" + du + " after scanning " + numTuples
                        + " tuples.");
            }
        }

        return primaryDomain == null ? (maxAlexaRankDomain == null ? mostUsedDomain : maxAlexaRankDomain)
                : primaryDomain;
    }

    private String getStringAt(TupleEntry arguments, int pos) {
        Object obj = arguments.getObject(pos);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Utf8) {
            return obj.toString();
        }
        return (String) obj;
    }

    private void setArgPosMap(TupleEntry arguments) {
        if (duArgIdx == -1 || domainArgIdx == -1 || dunsArgIdx == -1 || alexaRankArgIdx == -1) {
            Fields fields = arguments.getFields();
            if (duArgIdx == -1) {
                duArgIdx = fields.getPos(duField);
            }
            if (domainArgIdx == -1) {
                domainArgIdx = fields.getPos(domainField);
            }
            if (dunsArgIdx == -1) {
                dunsArgIdx = fields.getPos(dunsField);
            }
            if (alexaRankArgIdx == -1) {
                alexaRankArgIdx = fields.getPos(alexaRankField);
            }
        }
    }
}
