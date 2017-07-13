package com.latticeengines.datacloud.dataflow.refresh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterRebuildParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterSourceParameters;


@Component("accountMasterRebuildFlow")
public class AccountMasterRebuildFlow extends TypesafeDataFlowBuilder<AccountMasterRebuildParameters> {
    private static final Logger log = LoggerFactory.getLogger(AccountMasterRebuildFlow.class);

    private static final String JOIN_KEY_PREFIX = "LDC_AM_JOINKEY_";
    private static final String SECOND_KEY_SUFFIX = "_SCND_KEY";

    @Override
    public Node construct(AccountMasterRebuildParameters parameters) {

        log.info("Add account master seed");

        Node seed = addSource(parameters.getBaseTables().get(0));
        
        String[] seedFields = parameters.getSeedFields();
        
        log.info("Retain seed fields " + Arrays.toString(seedFields));
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

        Node firstSource = buildInputSources(parameters, domainBased, dunsBased, joinKeyMap);


        Node joined = seed;
        if (firstSource != null) {
            log.info("Join first source");
            FieldList joinField = new FieldList(joinKeyMap.get(firstSource));
            Node deduped = firstSource.groupByAndLimit(joinField, 1); 
            joined = joined.join(new FieldList(dunsKey), deduped, joinField, JoinType.LEFT);
        }

        String id = parameters.getId().get(0);
        Node domainConverged = convergeSources(seed, domainBased, domainKey, id, joinKeyMap);

        if (domainConverged != null) {
            joined = joined.leftJoin(new FieldList(id), domainConverged, new FieldList(id));
        }


        Node dunsConverged = convergeSources(seed, dunsBased, dunsKey, id, joinKeyMap);
        if (dunsConverged != null) {
            joined = joined.leftJoin(new FieldList(id), dunsConverged, new FieldList(joinKeyMap.get(dunsConverged)));
        }

        FieldList joinFields = getJoinFields(joined);
        Node discarded = joined.discard(joinFields);
        Node stamped = discarded.addTimestamp(parameters.getTimestampField());

        Node renamed = renameColumns(stamped);
        return renamed;
    }

    private Node convergeSources(Node seed, List<Node> nodes, String joinKey, String id, Map<Node, String> joinKeyMap) {

        if (nodes.size() == 0) {
            log.info("No source for " + joinKey);
            return null;
        }


        Node filteredSeed = seed.filter(joinKey + " != null", new FieldList(joinKey));

        Node retainedSeed = filteredSeed.retain(new FieldList(id, joinKey));


        List<FieldList> groupFieldLists = new ArrayList<FieldList>();
        for (Node node : nodes) {
            String groupKey = joinKeyMap.get(node);
            groupFieldLists.add(new FieldList(groupKey));
        }

        Node coGrouped = retainedSeed.coGroup(new FieldList(joinKey), nodes, groupFieldLists, JoinType.OUTER);

        Node filtered = coGrouped.filter(id + " != null", new FieldList(id));

        return filtered;
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
            String sourceName = sourceParameters.getSourceName();
            Node node = convertSource(sourceParameters, joinKeyMap);
            log.info("Converting source " + sourceName);
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
        log.info("New Join Key " + JOIN_KEY_PREFIX + joinKeyMap.size());
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

    @SuppressWarnings("serial")
    private Node renameColumns(Node node) {
        Map<String, String> renamedColumns = new HashMap<String, String>() {
            {
                put("Domain", "LDC_Domain");
                put("Name", "LDC_Name");
                put("City", "LDC_City");
                put("State", "LDC_State");
                put("Country", "LDC_Country");
                put("DUNS", "LDC_DUNS");
                put("Street", "LDC_Street");
                put("ZipCode", "LDC_ZipCode");
                put("PrimaryIndustry", "LDC_PrimaryIndustry");
                put("DomainSource", "LDC_DomainSource");
            }
        };
        List<String> oldNames = node.getFieldNames();
        List<String> newNames = new ArrayList<String>();
        for (String oldName : oldNames) {
            if (!renamedColumns.containsKey(oldName)) {
                newNames.add(oldName);
            } else {
                newNames.add(renamedColumns.get(oldName));
            }
        }
        node = node.rename(new FieldList(oldNames), new FieldList(newNames));
        return node;
    }
}
