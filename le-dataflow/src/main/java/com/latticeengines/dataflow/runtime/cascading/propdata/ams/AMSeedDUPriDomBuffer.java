package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dataflow.runtime.cascading.BaseGroupbyBuffer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dataflow.operations.OperationCode;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.domain.exposed.dataflow.operations.OperationMessage;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedDUPriDomBuffer extends BaseGroupbyBuffer {

    public static final String DU_PRIMARY_DOMAIN = "DUPrimaryDomain";
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AMSeedDUPriDomBuffer.class);
    private String duField = DataCloudConstants.ATTR_DU_DUNS;
    private String dunsField = DataCloudConstants.AMS_ATTR_DUNS;
    private String domainField = DataCloudConstants.AMS_ATTR_DOMAIN;
    private String alexaRankField = DataCloudConstants.ATTR_ALEXA_RANK;
    private int duArgIdx = -1;
    private int dunsArgIdx = -1;
    private int domainArgIdx = -1;
    private int alexaRankArgIdx = -1;

    // output (DU, PrimaryDomain)
    public AMSeedDUPriDomBuffer(Fields fieldDeclaration) {
        // Input node has discarded LE_OperationLogs if it exists
        super(fieldDeclaration, true);
    }

    @Override
    protected Tuple setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        Pair<String, String> selectedDomAndReason = findPrimaryOrMostUsedDomain(argumentsInGroup);
        result = outputPrimaryDomain(result, selectedDomAndReason.getLeft(), selectedDomAndReason.getRight());
        return result;
    }

    /*
     * If DUDuns is blank, skip the group. Just safe guard.
     */
    @Override
    protected boolean shouldSkipGroup(TupleEntry group) {
        return StringUtils.isBlank(group.getString(0));
    }
    
    private Tuple outputPrimaryDomain(Tuple result, String primaryDomain, String selectedReason) {
        Integer pdLoc = namePositionMap.get(DU_PRIMARY_DOMAIN);
        if (StringUtils.isNotBlank(primaryDomain)) {
            // we have most used domain, but that tuple does not satisfy DUNS=DU
            result.set(pdLoc, primaryDomain);
            result.set(logFieldIdx, OperationLogUtils.buildLog(DataCloudConstants.TRANSFORMER_AMS_FILL_DOM_IN_DU,
                    OperationCode.FILL_DOM_IN_DU, selectedReason));
            return result;
        } else {
            result.set(pdLoc, null);
            return result;
        }
    }

    /**
     * @param argumentsIter
     * @return <selected domain, selection reason>
     */
    private Pair<String, String> findPrimaryOrMostUsedDomain(Iterator<TupleEntry> argumentsIter) {
        String mostUsedDomain = null;
        String primaryDomain = null;
        String selectedReason = null;

        Map<String, Integer> domainCountMap = new HashMap<>();
        int maxCount = 0;
        int numTuples = 0;
        String du = null;
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
                log.warn("The group of DU=" + du + " has " + numTuples
                        + " tuples. No primary domain.");
            } else {
                log.warn("Found a primary domain for the group of DU=" + du + " after scanning "
                        + numTuples + " tuples.");
            }
        }

        if (primaryDomain != null) {
            selectedReason = OperationMessage.DU_DOMAIN;
        } else if (maxAlexaRankDomain != null) {
            primaryDomain = maxAlexaRankDomain;
            selectedReason = String.format(OperationMessage.LOW_ALEXA_RANK, maxAlexaRank);
        } else {
            primaryDomain = mostUsedDomain;
            if (primaryDomain != null) {
                selectedReason = String.format(OperationMessage.HIGH_OCCUR, maxCount);
            }
        }
        return Pair.of(primaryDomain, selectedReason);
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
