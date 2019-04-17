package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterNetNewBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccountMasterNetNewConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

import cascading.tuple.Fields;

@Component("accountMasterNetNewFlow")
public class AccountMasterNetNewFlow extends ConfigurableFlowBase<AccountMasterNetNewConfig> {

    private AccountMasterNetNewConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);

        Node accountMaster = addSource(parameters.getBaseTables().get(0));

        Node filteredSource = addFilterNode(accountMaster);

        Node retainedColNode = filteredSource.retain(new FieldList(config.getResultFields()));

        Node groupByNode = groupingNode(retainedColNode);

        return groupByNode;
    }

    private Node groupingNode(Node retainedColNode) {
        List<String> fieldNames = retainedColNode.getFieldNames();
        AccountMasterNetNewBuffer buffer = new AccountMasterNetNewBuffer(
                new Fields(fieldNames.toArray(new String[fieldNames.size()])));
        FieldList groupByFieldList = new FieldList(config.getGroupBy());
        Node groupByNode = retainedColNode.groupByAndBuffer(groupByFieldList, buffer);
        return groupByNode;
    }

    private Node addFilterNode(Node accountMaster) {
        Node filterNode = accountMaster;

        for (String filterField : config.getFilterCriteria().keySet()) {
            FieldList filterColumn = new FieldList(filterField);

            filterNode = filterNode.filter(filterField + " != null", filterColumn);

            String expression = "";
            boolean firstPass = true;

            for (String criteria : config.getFilterCriteria().get(filterField)) {
                expression += firstPass ? "" : " || ";

                firstPass = false;

                expression += filterField + ".equals(\"" + criteria + "\")";
            }

            filterNode = filterNode.filter(expression, filterColumn);
        }
        return filterNode;
    }

    @Override
    public String getDataFlowBeanName() {
        return "accountMasterNetNewFlow";
    }

    @Override
    public String getTransformerName() {
        return "accountMasterNetNewTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AccountMasterNetNewConfig.class;
    }
}
