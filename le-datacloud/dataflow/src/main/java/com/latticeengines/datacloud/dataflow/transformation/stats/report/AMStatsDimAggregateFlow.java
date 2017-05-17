package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsDimAggregateFlow")
public class AMStatsDimAggregateFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        String[] dimensionIdFieldNames = dimensionDefinitionMap.keySet()
                .toArray(new String[dimensionDefinitionMap.size()]);

        return createDimensionBasedAggregateNode(node, dimensionIdFieldNames);
    }

    private Node createDimensionBasedAggregateNode(Node node, String[] dimensionIdFieldNames) {

        Fields fields = new Fields();
        List<String> groupBy = new ArrayList<>();
        int idx = 0;
        String[] allFields = new String[node.getSchema().size()];
        for (FieldMetadata fieldMeta : node.getSchema()) {
            String name = fieldMeta.getFieldName();
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

        return node.groupByAndAggregate(new FieldList(groupBy), aggregator, fms);
    }
}
