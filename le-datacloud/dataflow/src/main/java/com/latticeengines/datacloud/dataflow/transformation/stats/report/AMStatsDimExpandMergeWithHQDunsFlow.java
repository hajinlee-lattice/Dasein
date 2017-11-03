package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionExpandWithHQDunsBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsDimExpandMergeWithHQDunsFlow")
public class AMStatsDimExpandMergeWithHQDunsFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        String[] dimensionIdFieldNames = dimensionDefinitionMap.keySet()
                .toArray(new String[dimensionDefinitionMap.size()]);

        return createDimensionBasedExpandAndMergeNodes(node, parameters, //
                dimensionDefinitionMap, dimensionIdFieldNames);
    }

    @SuppressWarnings("unchecked")
    private Node createDimensionBasedExpandAndMergeNodes(Node node, //
            AccountMasterStatsParameters parameters, //
            Map<String, List<String>> dimensionDefinitionMap, //
            String[] dimensionIdFieldNames) {
        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(node.getSchema());

        List<String> fields = (List<String>) Arrays.asList(node.getFieldNamesArray());
        List<String> dimensionFields = (List<String>) Arrays.asList(dimensionIdFieldNames);

        AMStatsDimensionExpandWithHQDunsBuffer.Params functionParams = //
                new AMStatsDimensionExpandWithHQDunsBuffer.Params(//
                        new Fields(node.getFieldNamesArray()), //
                        fields, //
                        dimensionFields);

        AMStatsDimensionExpandWithHQDunsBuffer buffer = //
                new AMStatsDimensionExpandWithHQDunsBuffer(functionParams);

        return node.groupByAndBuffer(new FieldList(dimensionIdFieldNames), //
                buffer, fms);
    }
}
