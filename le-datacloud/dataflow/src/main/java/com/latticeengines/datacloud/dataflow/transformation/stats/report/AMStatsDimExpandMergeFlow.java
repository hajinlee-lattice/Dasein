package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionExpandBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsDimExpandMergeFlow")
public class AMStatsDimExpandMergeFlow extends AMStatsFlowBase {

    private static Log log = LogFactory.getLog(AMStatsDimExpandMergeFlow.class);

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        String[] dimensionIdFieldNames = dimensionDefinitionMap.keySet()
                .toArray(new String[dimensionDefinitionMap.size()]);

        node = createDimensionBasedAggregateNode(node, dimensionIdFieldNames);

        node = createDimensionBasedExpandAndMergeNodes(node, parameters, //
                dimensionDefinitionMap, dimensionIdFieldNames);

        return node;
    }

    private Node createDimensionBasedAggregateNode(Node node, String[] dimensionIdFieldNames) {

        Fields fields = new Fields();
        List<String> groupBy = new ArrayList<>();
        int idx = 0;
        String[] allFields = new String[node.getSchema().size()];
        for (FieldMetadata fieldMeta : node.getSchema()) {
            String name = fieldMeta.getFieldName();
            if (name.equals("_Location_") || name.equals("_Industry_")) {
                log.info("DimensionName " + name + " Position " + idx);
            }
            allFields[idx++] = name;
            fields = fields.append(new Fields(name, fieldMeta.getJavaType()));
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
        AMStatsDimensionAggregator aggregator = //
                new AMStatsDimensionAggregator(fields);
        node = node.groupByAndAggregate(new FieldList(groupBy), aggregator, fms);

        return node;
    }

    private Node createDimensionBasedExpandAndMergeNodes(Node node, //
            AccountMasterStatsParameters parameters, //
            Map<String, List<String>> dimensionDefinitionMap, //
            String[] dimensionIdFieldNames) {
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            List<String> groupBy = new ArrayList<>();
            for (String dimensionIdFieldName : dimensionDefinitionMap.keySet()) {
                if (!dimensionIdFieldName.equals(dimensionKey)) {
                    groupBy.add(dimensionIdFieldName);
                }
            }

            Fields expandFields = new Fields();
            List<FieldMetadata> targetField = new ArrayList<>();
            String[] allFields = new String[node.getSchema().size()];

            int idx = 0;
            for (FieldMetadata s : node.getSchema()) {
                expandFields = expandFields.append(new Fields(s.getFieldName()));
                allFields[idx++] = s.getFieldName();
                targetField.add(s);
            }

            List<String> attrList = new ArrayList<>();
            List<Integer> attrIdList = new ArrayList<>();

            findAttributeIds(node.getSchema(), attrList, attrIdList);

            Fields allLeafFields = new Fields();
            for (FieldMetadata fieldMeta : node.getSchema()) {
                allLeafFields = allLeafFields.append(new Fields(fieldMeta.getFieldName(), fieldMeta.getJavaType()));
            }

            List<FieldMetadata> fms = new ArrayList<>();
            fms.addAll(node.getSchema());

            AMStatsDimensionExpandBuffer.Params functionParams = //
                    new AMStatsDimensionExpandBuffer.Params(//
                            dimensionKey, dimensionDefinitionMap, //
                            allLeafFields, parameters.getRequiredDimensionsValuesMap());
            AMStatsDimensionExpandBuffer buffer = //
                    new AMStatsDimensionExpandBuffer(functionParams);

            node = node.retain(new FieldList(allFields));
            node = node.groupByAndBuffer(new FieldList(groupBy), //
                    buffer, fms);
        }
        return node;
    }

    private void findAttributeIds(List<FieldMetadata> finalLeafSchema, List<String> attrList,
            List<Integer> attrIdList) {
        int pos = 0;
        for (FieldMetadata field : finalLeafSchema) {
            attrList.add(field.getFieldName());
            attrIdList.add(pos++);
        }
    }
}
