package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;

@Component("matchCdlAccountFlow")
public class MatchCdlAccountFlow extends TypesafeDataFlowBuilder<MatchCdlAccountParameters> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlAccountFlow.class);

    @Override
    public Node construct(MatchCdlAccountParameters parameters) {
        Node inputTable = addSource(parameters.inputTable);
        Node accountTable = addSource(parameters.accountTable);

        List<String> retainFields = buildRetainFields(parameters, inputTable, accountTable);
        FieldList inputMatchFields = new FieldList(parameters.getInputMatchFields());
        FieldList accountMatchFields = new FieldList(parameters.getAccountMatchFields());
        Node result = null;
        if (!parameters.isRightJoin()) {
            result = inputTable.join(inputMatchFields, accountTable, accountMatchFields, JoinType.LEFT);
        } else {
            List<String> inputFields = inputTable.getFieldNames();
            String lidFieldName = parameters.getAccountMatchFields().get(0);
            FieldList LidField = new FieldList(lidFieldName);
            Node nullLidNode = inputTable.filter(lidFieldName + " == null", LidField);
            Node notNullLidNode = inputTable.filter(lidFieldName + " != null", LidField);
            
            String newLidField = lidFieldName + "_NEW_";
            accountTable = accountTable.rename(LidField, new FieldList(newLidField));
            accountTable = accountTable.retain(new FieldList(accountTable.getFieldNames()));
            result = accountTable.join(new FieldList(newLidField), notNullLidNode, inputMatchFields, JoinType.RIGHT);
            result = result.retain(new FieldList(inputFields));
            result = result.merge(nullLidNode);
            if (parameters.isDedupe()) {
                result = result.groupByAndLimit(accountMatchFields, 1);
            }
        }

        result = result.retain(new FieldList(retainFields));
        log.info("Match Cdl Account table's columns=" + StringUtils.join(retainFields, ","));
        return result;
    }

    private List<String> buildRetainFields(MatchCdlAccountParameters parameters, Node inputTable, Node accountTable) {
        List<String> retainFields = new ArrayList<>(inputTable.getFieldNames());
        accountTable.getFieldNames().forEach(attr -> {
            if (!retainFields.contains(attr))
                retainFields.add(attr);
        });
        retainFields.removeAll(Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name()));
        return retainFields;
    }

}
