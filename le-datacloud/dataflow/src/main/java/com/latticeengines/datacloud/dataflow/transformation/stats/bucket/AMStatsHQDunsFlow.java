package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDomainBckFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsHQDunsFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;

import cascading.tuple.Fields;

@Component("amStatsHQDunsFlow")
public class AMStatsHQDunsFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        node = node.addColumnWithFixedValue(//
                AccountMasterStatsParameters.DOMAIN_BCK_FIELD, null, String.class);

        node = node.addColumnWithFixedValue(//
                AccountMasterStatsParameters.HQ_DUNS, null, String.class);

        node = generateDomainBckNode(node);

        return generateHQDunsNode(node);
    }

    private Node generateDomainBckNode(Node node) {
        node.renamePipe("beginDomainBckFlow");

        Node nodeWithoutProperDomain = node.filter(//
                AccountMasterStatsParameters.DOMAIN + " == null ", //
                new FieldList(AccountMasterStatsParameters.DOMAIN));

        Node nodeWithProperDomain = node.filter(//
                AccountMasterStatsParameters.DOMAIN + " != null ", //
                new FieldList(AccountMasterStatsParameters.DOMAIN));

        nodeWithProperDomain = addDomainBckValues(nodeWithProperDomain);

        return nodeWithoutProperDomain.merge(nodeWithProperDomain);
    }

    private Node addDomainBckValues(Node nodeWithProperDomain) {
        AMStatsDomainBckFunction.Params functionParams = //
                new AMStatsDomainBckFunction.Params(//
                        new Fields(nodeWithProperDomain.getFieldNames()
                                .toArray(new String[nodeWithProperDomain.getFieldNames().size()])), //
                        AccountMasterStatsParameters.DOMAIN, //
                        AccountMasterStatsParameters.DOMAIN_BCK_FIELD);

        AMStatsDomainBckFunction hqDunsCalculationFunction = //
                new AMStatsDomainBckFunction(functionParams);

        return nodeWithProperDomain.apply(hqDunsCalculationFunction, //
                getFieldList(nodeWithProperDomain.getSchema()), //
                nodeWithProperDomain.getSchema(), //
                getFieldList(nodeWithProperDomain.getSchema()), //
                Fields.REPLACE);
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

        return nodeWithoutProperCodes.merge(nodeWithProperCodes);
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
                Fields.REPLACE);
    }
}
