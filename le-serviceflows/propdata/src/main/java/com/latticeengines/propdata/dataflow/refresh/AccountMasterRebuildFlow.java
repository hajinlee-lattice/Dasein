package com.latticeengines.propdata.dataflow.refresh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.AccountMasterRebuildParameters;
import com.latticeengines.domain.exposed.propdata.dataflow.AccountMasterSourceParameters;


@Component("accountMasterRebuildFlow")
public class AccountMasterRebuildFlow extends TypesafeDataFlowBuilder<AccountMasterRebuildParameters> {
    private static final Log log = LogFactory.getLog(AccountMasterRebuildFlow.class);

    private static final String JOIN_KEY_PREFIX = "LDC_AM_JOINKEY_";
    private static final String SECOND_KEY_SUFFIX = "_SCND_KEY";

    @Override
    public Node construct(AccountMasterRebuildParameters parameters) {

        log.info("Add account master seed");

        Node seed = addSource(parameters.getBaseTables().get(0));
        
        String[] seedFields = parameters.getSeedFields();
        log.info("Retain seed fields " + seedFields);
        List<String> fieldList = new ArrayList<String>();
        for (int i = 0; i< seedFields.length; i++) {
             fieldList.add(seedFields[i]);
        }
        seed = seed.retain(new FieldList(fieldList));

        String domainKey = parameters.getDomainKey();
        String dunsKey = parameters.getDunsKey();

        Map<Node, String> joinKeyMap = new HashMap<Node, String>();

        List<Node> domainBased = new ArrayList<Node>();
        List<Node> dunsBased = new ArrayList<Node>();

        Node lastSource = buildInputSources(parameters, domainBased, dunsBased, joinKeyMap);

        Node domainConverged = convergeSources(domainBased, joinKeyMap);
        Node dunsConverged = convergeSources(dunsBased, joinKeyMap);

        FieldList domainField = new FieldList(domainKey);
        FieldList dunsField = new FieldList(dunsKey);

        Node joined = seed;
        if (domainConverged != null) {
            String joinKey = joinKeyMap.get(domainConverged);
            joined = joined.join(domainField, domainConverged, new FieldList(joinKey), JoinType.LEFT);
        }
        if (dunsConverged != null) {
            String joinKey = joinKeyMap.get(dunsConverged);
            joined = joined.join(dunsField, dunsConverged, new FieldList(joinKey), JoinType.LEFT);
        }

        if (lastSource != null) {
            log.info("Join final source");
            String joinKey = joinKeyMap.get(lastSource);
            joined = joined.join(dunsField, lastSource, new FieldList(joinKey), JoinType.LEFT);
            String secondKey = joinKey + SECOND_KEY_SUFFIX;
            String filterFunc = domainKey + " == null || " + domainKey + " == " + secondKey;
            joined = joined.filter(filterFunc, new FieldList(domainKey, secondKey));

        }

        FieldList joinFields = getJoinFields(joined);
        Node discarded = joined.discard(joinFields);
        Node stamped = discarded.addTimestamp(parameters.getTimestampField()); 

        return stamped; 
    }

    private Node convergeSources(List<Node> nodes, Map<Node, String> joinKeyMap) {

        if (nodes.size() == 0) {
            return null;
        }

        List<Node> convergedSources = nodes;

        while (convergedSources.size() > 1) {
            List<Node> sources = new ArrayList<Node>();
            Node prevSource = null;
            log.info("Converging sources " + convergedSources.size());
            for (Node source : convergedSources) {
                 if (prevSource == null) {
                     prevSource = source;
                 } else {

                     String leftJoinKey = joinKeyMap.get(source);
                     String rightJoinKey = joinKeyMap.get(prevSource);
                     Node joined = source.join(new FieldList(leftJoinKey), prevSource, new FieldList(rightJoinKey), JoinType.OUTER);

                     String mergeFunc = leftJoinKey + " == null ? " + rightJoinKey + " : " + leftJoinKey;
                     String mergedJoinKey = newJoinKey(joinKeyMap);
                     FieldMetadata joinMeta = new FieldMetadata(mergedJoinKey, String.class);
                     Node merged = joined.addFunction(mergeFunc, new FieldList(leftJoinKey, rightJoinKey), joinMeta);

                     joinKeyMap.put(merged, mergedJoinKey);
                     sources.add(merged);

                     prevSource = null;
                 }
            }
            if (prevSource != null) {
                sources.add(prevSource);
            }
            convergedSources = sources;
        }

        Node converged = convergedSources.get(0);

        return converged;
    }

    private Node convertSource(AccountMasterSourceParameters source, Map<Node, String> joinKeyMap) {

        log.info("Convert source " + source.getSourceName());

        Node node = addSource(source.getSourceName());
        source.filterAttrs(node.getFieldNames());

        List<String> sourceAttrs = source.getSourceAttrs();
        List<String> outputAttrs = source.getOutputAttrs();

        String joinKey = source.getJoinKey();

        String outputJoinKey = newJoinKey(joinKeyMap);
        sourceAttrs.add(joinKey);
        outputAttrs.add(outputJoinKey);

        String secondKey = source.getSecondKey();
        if (secondKey != null) {
            String outputSecondKey = outputJoinKey + SECOND_KEY_SUFFIX;
            sourceAttrs.add(secondKey);
            outputAttrs.add(outputSecondKey);
        }

        Node retained = node.retain(new FieldList(sourceAttrs));
        Node renamed = retained.rename(new FieldList(sourceAttrs), new FieldList(outputAttrs));

        joinKeyMap.put(renamed, outputJoinKey);

        return renamed;
    }

    private Node buildInputSources(AccountMasterRebuildParameters parameters, List<Node> domainBased, List<Node> dunsBased, Map<Node, String> joinKeyMap) {
        Node lastSource = null;

        String lastSourceName = parameters.getLastSource();
        for (AccountMasterSourceParameters sourceParameters : parameters.getSourceParameters()) {
            Node node = convertSource(sourceParameters, joinKeyMap);
            String sourceName = sourceParameters.getSourceName();
            if (sourceName.equals(lastSourceName)) {
                log.info("Add last source to process " + sourceName);
                lastSource = node;
            } else {
                if (sourceParameters.getSourceType() == AccountMasterSourceParameters.DomainBased) {
                    domainBased.add(node);
                } else {
                    dunsBased.add(node);
                }
            }
        }
        return lastSource;
    }

    private String newJoinKey(Map<Node, String> joinKeyMap) {
        return JOIN_KEY_PREFIX + joinKeyMap.size();
    }

    private FieldList getJoinFields(Node node) {
        List<String> fieldNames = node.getFieldNames();
        List<String> joinList = new ArrayList<String>();
        for (String field : fieldNames) {
            if (field.startsWith(JOIN_KEY_PREFIX)) {
                joinList.add(field);
            }
        }
        return new FieldList(joinList);
    }
}
