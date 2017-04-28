package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsHQDunsFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;

import cascading.tuple.Fields;

@Component("amStatsHQDunsFlow")
public class AMStatsHQDunsFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        node = node.addColumnWithFixedValue(//
                AccountMasterStatsParameters.HQ_DUNS, null, String.class);

        return generateHQDunsNode(node);
    }

    private Node generateHQDunsNode(Node node) {
        node.renamePipe("beginHQDunsFlow");

        Node nodeWithoutProperCodes = node.filter(//
                AccountMasterStatsParameters.STATUS_CODE + " == null || " //
                        + AccountMasterStatsParameters.SUBSIDIARY_INDICATOR + " == null || " //
                        + AccountMasterStatsParameters.DUNS + " == null", //
                new FieldList(AccountMasterStatsParameters.STATUS_CODE, //
                        AccountMasterStatsParameters.SUBSIDIARY_INDICATOR, //
                        AccountMasterStatsParameters.DUNS));

        Node nodeWithProperCodes = node.filter(//
                AccountMasterStatsParameters.STATUS_CODE + " != null && " //
                        + AccountMasterStatsParameters.SUBSIDIARY_INDICATOR + " != null && " //
                        + AccountMasterStatsParameters.DUNS + " != null", //
                new FieldList(AccountMasterStatsParameters.STATUS_CODE,
                        AccountMasterStatsParameters.SUBSIDIARY_INDICATOR, //
                        AccountMasterStatsParameters.DUNS));

        nodeWithProperCodes = addHQDunsValues(nodeWithProperCodes);

        Node mergedNodes = nodeWithoutProperCodes.merge(nodeWithProperCodes);

        return mergedNodes;
    }

    private Node addHQDunsValues(Node nodeWithProperCodes) {
        AMStatsHQDunsFunction.Params functionParams = //
                new AMStatsHQDunsFunction.Params(//
                        new Fields(nodeWithProperCodes.getFieldNames()
                                .toArray(new String[nodeWithProperCodes.getFieldNames().size()])), //
                        AccountMasterStatsParameters.STATUS_CODE, //
                        AccountMasterStatsParameters.SUBSIDIARY_INDICATOR, //
                        AccountMasterStatsParameters.DUNS, //
                        AccountMasterStatsParameters.DDUNS, //
                        AccountMasterStatsParameters.GDUNS, //
                        AccountMasterStatsParameters.HQ_DUNS);

        AMStatsHQDunsFunction hqDunsCalculationFunction = //
                new AMStatsHQDunsFunction(functionParams);

        return nodeWithProperCodes.apply(hqDunsCalculationFunction, //
                getFieldList(nodeWithProperCodes.getSchema()), //
                nodeWithProperCodes.getSchema(), //
                getFieldList(nodeWithProperCodes.getSchema()), //
                Fields.REPLACE
                );
    }
}
