package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDedupAggRollupWithHQDuns;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDedupAggRollupWithHQDuns.Params;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;
import edu.emory.mathcs.backport.java.util.Arrays;

@Component("amStatsDimAggregateWithHQDunsFlow")
public class AMStatsDimAggregateWithHQDunsFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        String[] dimensionIdFieldNames = dimensionDefinitionMap.keySet()
                .toArray(new String[dimensionDefinitionMap.size()]);

        return createDimensionBasedAggregateNode(node, parameters, dimensionIdFieldNames);
    }

    @SuppressWarnings("unchecked")
    private Node createDimensionBasedAggregateNode(Node node, //
            AccountMasterStatsParameters parameters, //
            String[] dimensionIdFieldNames) {

        List<String> groupBy = new ArrayList<>();
        groupBy.add(AccountMasterStatsParameters.HQ_DUNS);
        groupBy.add(AccountMasterStatsParameters.DOMAIN_BCK_FIELD);

        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(node.getSchema());

        List<String> fields = (List<String>) Arrays.asList(node.getFieldNamesArray());
        List<String> dimensionFields = (List<String>) Arrays.asList(dimensionIdFieldNames);

        AMStatsDedupAggRollupWithHQDuns.Params params = //
                new Params(fields, //
                        dimensionFields, //
                        groupBy, //
                        new Fields(node.getFieldNamesArray()), //
                        parameters.getRequiredDimensionsValuesMap());

        AMStatsDedupAggRollupWithHQDuns buffer = new AMStatsDedupAggRollupWithHQDuns(params);
        node = node.groupByAndBuffer(new FieldList(groupBy), buffer, fms);

        return node.discard(groupBy.toArray(new String[groupBy.size()]));
    }
}
