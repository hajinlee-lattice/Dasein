package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDedupAggRollupWithHQDuns;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDedupAggRollupWithHQDuns.Params;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDedupAggRollupWithHQDunsMarker;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionExpandBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsDimAggregateWithHQDunsFlow")
public class AMStatsDimAggregateWithHQDunsFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        String[] dimensionIdFieldNames = dimensionDefinitionMap.keySet()
                .toArray(new String[dimensionDefinitionMap.size()]);

        FieldList flagForHQDunsProcIndicator = //
                new FieldList(AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR);

        node = node.addColumnWithFixedValue(AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR, //
                Boolean.FALSE, Boolean.class);

        node = markRowsValidForHQDunsBasedDedupProcessing(node);

        Node nodeWithProperHQDuns = createHQDunsBasedNode(parameters, node, dimensionIdFieldNames,
                flagForHQDunsProcIndicator);

        Node nodeWithoutProperHQDuns = createNodeWithoutProperHQDuns(parameters, node, dimensionDefinitionMap,
                dimensionIdFieldNames, flagForHQDunsProcIndicator);

        node = nodeWithProperHQDuns.merge(nodeWithoutProperHQDuns);

        return node;
    }

    private Node createNodeWithoutProperHQDuns(AccountMasterStatsParameters parameters, Node node,
            Map<String, List<String>> dimensionDefinitionMap, String[] dimensionIdFieldNames,
            FieldList flagForHQDunsProcIndicator) {
        Node nodeWithoutProperHQDuns = node.filter(
                AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR + "==" + Boolean.FALSE.toString(),
                new FieldList(AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR));
        nodeWithoutProperHQDuns = nodeWithoutProperHQDuns.discard(flagForHQDunsProcIndicator);

        nodeWithoutProperHQDuns = createDimensionBasedAggregateNode(nodeWithoutProperHQDuns, //
                dimensionIdFieldNames);

        nodeWithoutProperHQDuns = createDimensionBasedExpandAndMergeNodes(nodeWithoutProperHQDuns, //
                parameters, dimensionDefinitionMap);
        return nodeWithoutProperHQDuns;
    }

    private Node createHQDunsBasedNode(AccountMasterStatsParameters parameters, Node node,
            String[] dimensionIdFieldNames, FieldList flagForHQDunsProcIndicator) {
        Node nodeWithProperHQDuns = node.filter(
                AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR + "==" + Boolean.TRUE.toString(),
                new FieldList(AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR));
        nodeWithProperHQDuns = nodeWithProperHQDuns.discard(flagForHQDunsProcIndicator);

        nodeWithProperHQDuns = createHQDunsBasedNode(nodeWithProperHQDuns, //
                parameters, dimensionIdFieldNames);
        return nodeWithProperHQDuns;
    }

    private Node createDimensionBasedAggregateNode(Node node, String[] dimensionIdFieldNames) {
        List<String> hqDunsFields = getHQDunsFields();
        node = node.discard(hqDunsFields.toArray(new String[hqDunsFields.size()]));

        List<String> groupBy = new ArrayList<>();
        int idx = 0;
        String[] allFields = new String[node.getSchema().size()];
        for (FieldMetadata fieldMeta : node.getSchema()) {
            String name = fieldMeta.getFieldName();
            allFields[idx++] = name;
            for (String dimensionId : dimensionIdFieldNames) {
                if (name.equals(dimensionId)) {
                    groupBy.add(name);
                    break;
                }
            }
        }
        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(node.getSchema());

        node = node.retain(new FieldList(allFields));
        AMStatsDimensionAggregator aggregator = new AMStatsDimensionAggregator(new Fields(allFields));
        return node.groupByAndAggregate(new FieldList(groupBy), aggregator, fms);
    }

    private List<String> getHQDunsFields() {
        List<String> groupBy = new ArrayList<>();
        groupBy.add(AccountMasterStatsParameters.HQ_DUNS);
        groupBy.add(AccountMasterStatsParameters.DOMAIN_BCK_FIELD);
        return groupBy;
    }

    private Node createDimensionBasedExpandAndMergeNodes(Node node, //
            AccountMasterStatsParameters parameters, //
            Map<String, List<String>> dimensionDefinitionMap) {
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            List<String> groupBy = new ArrayList<>();
            for (String dimensionIdFieldName : dimensionDefinitionMap.keySet()) {
                if (!dimensionIdFieldName.equals(dimensionKey)) {
                    groupBy.add(dimensionIdFieldName);
                }
            }

            Fields expandFields = new Fields();
            String[] allFields = new String[node.getSchema().size()];

            int idx = 0;
            for (FieldMetadata s : node.getSchema()) {
                expandFields = expandFields.append(new Fields(s.getFieldName()));
                allFields[idx++] = s.getFieldName();
            }

            Fields allLeafFields = new Fields(node.getFieldNamesArray());
            List<FieldMetadata> fms = new ArrayList<>();
            fms.addAll(node.getSchema());

            AMStatsDimensionExpandBuffer.Params functionParams = //
                    new AMStatsDimensionExpandBuffer.Params(//
                            allLeafFields, //
                            dimensionKey, //
                            parameters.getRequiredDimensionsValuesMap());

            AMStatsDimensionExpandBuffer buffer = //
                    new AMStatsDimensionExpandBuffer(functionParams);

            node = node.retain(new FieldList(allFields));
            node = node.groupByAndBuffer(new FieldList(groupBy), buffer, fms);
        }
        return node;
    }

    @SuppressWarnings("unchecked")
    private Node markRowsValidForHQDunsBasedDedupProcessing(Node node) {

        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(node.getSchema());

        List<String> fields = (List<String>) Arrays.asList(node.getFieldNamesArray());
        List<String> hqFields = getHQDunsFields();

        AMStatsDedupAggRollupWithHQDunsMarker.Params params = //
                new AMStatsDedupAggRollupWithHQDunsMarker.Params(new Fields(node.getFieldNamesArray()), //
                        fields, //
                        AccountMasterStatsParameters.HQ_DUNS_PROC_INDICATOR);

        AMStatsDedupAggRollupWithHQDunsMarker buffer = new AMStatsDedupAggRollupWithHQDunsMarker(params);
        return node.groupByAndBuffer(new FieldList(hqFields), buffer, fms);
    }

    @SuppressWarnings("unchecked")
    private Node createHQDunsBasedNode(Node node, //
            AccountMasterStatsParameters parameters, //
            String[] dimensionIdFieldNames) {

        List<String> groupBy = getHQDunsFields();

        List<FieldMetadata> fms = new ArrayList<>();
        for (FieldMetadata fm: node.getSchema()) {
            if (!groupBy.contains(fm.getFieldName())) {
                fms.add(fm);
            }
        }

        List<String> dimensionFields = Arrays.asList(dimensionIdFieldNames);

        AMStatsDedupAggRollupWithHQDuns.Params params = //
                new Params(dimensionFields, //
                        DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(fms)), //
                        parameters.getRequiredDimensionsValuesMap());

        AMStatsDedupAggRollupWithHQDuns buffer = new AMStatsDedupAggRollupWithHQDuns(params);
        return node.groupByAndBuffer(new FieldList(groupBy), buffer, fms);
    }
}
