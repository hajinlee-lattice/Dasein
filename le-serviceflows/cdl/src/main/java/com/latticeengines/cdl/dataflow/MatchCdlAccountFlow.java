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
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;

@Component("matchCdlAccountFlow")
public class MatchCdlAccountFlow extends TypesafeDataFlowBuilder<MatchCdlAccountParameters> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlAccountFlow.class);
    private static final String UUID = "__Id__";

    @Override
    public Node construct(MatchCdlAccountParameters parameters) {
        Node inputTable = addSource(parameters.inputTable);
        if (parameters.isRenameIdOnly()) {
            if (inputTable.getFieldNames().contains(InterfaceName.CustomerAccountId.name())) {
                return inputTable;
            }
            Node result = inputTable.rename(new FieldList(parameters.getInputMatchFields().get(0)),
                    new FieldList(InterfaceName.CustomerAccountId.name()));
            return result;
        }
        Node accountTable = addSource(parameters.accountTable);
        List<String> retainFields = buildRetainFields(parameters, inputTable, accountTable);
        FieldList inputMatchFields = new FieldList(parameters.getInputMatchFields());
        FieldList accountMatchFields = new FieldList(parameters.getAccountMatchFields());
        Node result = null;
        if (parameters.isHasAccountId()) {
            result = inputTable.join(inputMatchFields, accountTable, accountMatchFields, JoinType.LEFT);
        } else {
            String lidFieldName = parameters.getAccountMatchFields().get(0);
            FieldList lidField = new FieldList(lidFieldName);
            FieldMetadata lidMetadata = inputTable.getSchema(lidFieldName);
            String notExistingLid = "\"-11111\"";
            result = inputTable.apply(
                    String.format(lidFieldName + " != null ? " + lidFieldName + " : %s", notExistingLid), lidField,
                    lidMetadata);

            result = result.addUUID(UUID);
            result = result.join(lidField, accountTable, lidField, JoinType.LEFT);

            Node notNullLidNode = result.filter(String.format("!" + lidFieldName + ".equals(%s)", notExistingLid),
                    lidField);
            notNullLidNode = notNullLidNode.groupByAndLimit(new FieldList(UUID), 1);

            Node nullLidNode = result.filter(String.format(lidFieldName + ".equals(%s)", notExistingLid), lidField);
            result = notNullLidNode.merge(nullLidNode);

            result = result.apply(String.format(lidFieldName + ".equals(%s) ? null : " + lidFieldName, notExistingLid),
                    lidField, lidMetadata);
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
