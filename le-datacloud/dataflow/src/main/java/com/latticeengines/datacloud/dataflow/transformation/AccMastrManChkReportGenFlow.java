package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AmManChecksAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.AmReportGenerateParams;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(AccMastrManChkReportGenFlow.DATAFLOW_BEAN_NAME)
public class AccMastrManChkReportGenFlow extends TypesafeDataFlowBuilder<AmReportGenerateParams> {
    public static final String DATAFLOW_BEAN_NAME = "AccMastrManChkReportGenFlow";
    public static final String TRANSFORMER_NAME = "AccMastrManChkTransformer";

    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Node construct(AmReportGenerateParams parameters) {
        Node source1 = addSource(parameters.getBaseTables().get(0));
        Node source2 = addSource(parameters.getBaseTables().get(1));
        Node source3 = addSource(parameters.getBaseTables().get(2));
        Node source4 = addSource(parameters.getBaseTables().get(3));
        Node source5 = addSource(parameters.getBaseTables().get(4));
        Node source6 = addSource(parameters.getBaseTables().get(5));
        Node source7 = addSource(parameters.getBaseTables().get(6));

        // merging all nodes
        Node mergedNode = source1 //
                .merge(source2) //
                .merge(source3) //
                .merge(source4) //
                .merge(source5) //
                .merge(source6) //
                .merge(source7);

        // filtering messages for rowid and groupid = null
        String checkNullExpression = String.format("(%s == null && %s == null)", parameters.getRowId(),
                parameters.getGroupId());
        Node filteredRecords = mergedNode //
                .filter(checkNullExpression, new FieldList(mergedNode.getFieldNames())) //
                .retain(new FieldList(parameters.getCheckMessage()));

        // for row and group checks
        String chkRowGrpExp = String.format("(%s != null || %s != null)", parameters.getRowId(),
                parameters.getGroupId());
        Node rowGrpChecks = mergedNode //
                .filter(chkRowGrpExp, new FieldList(mergedNode.getFieldNames())) //
                .retain(new FieldList(parameters.getCheckCode(), parameters.getCheckField()));
        AmManChecksAggregator aggregator = new AmManChecksAggregator(parameters.getCheckCode(),
                parameters.getCheckField());
        rowGrpChecks = rowGrpChecks.groupByAndAggregate(
                new FieldList(parameters.getCheckCode(), parameters.getCheckField()), aggregator,
                Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class)));
        Node aggregatedChkResults = filteredRecords //
                .merge(rowGrpChecks);
        return aggregatedChkResults;
    }
}
