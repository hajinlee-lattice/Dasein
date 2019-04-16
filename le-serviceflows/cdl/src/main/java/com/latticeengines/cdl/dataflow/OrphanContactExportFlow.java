package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanContactExportParameters;

@Component(OrphanContactExportFlow.DATAFLOW_BEAN_NAME)
public class OrphanContactExportFlow extends TypesafeDataFlowBuilder<OrphanContactExportParameters> {
    public static final String DATAFLOW_BEAN_NAME = "orphanContactExportFlow";
    public static final String RENAME_PREFIX = "OrphanContactExport_";

    @Override
    public Node construct(OrphanContactExportParameters parameters) {
        Node srcContact = addSource(parameters.getContactTable());

        if (parameters.getAccountTable() == null) {
            return srcContact;
        }

        Node srcAccount = addSource(parameters.getAccountTable());
        List<String> validatedColumns = parameters.getValidatedColumns();

        List<String> retainFields = srcContact.getFieldNames().stream().filter(name -> validatedColumns.contains(name))
                .collect(Collectors.toList());
        // rename fields to avoid field conflicts
        String renamedAccountId = RENAME_PREFIX + InterfaceName.AccountId.name();
        srcAccount = renameFields(srcAccount, InterfaceName.AccountId.name(), renamedAccountId);

        srcContact = srcContact.join(new FieldList(InterfaceName.AccountId.name()),
                srcAccount, new FieldList(renamedAccountId), JoinType.LEFT);

        return srcContact
                .filter(String.format("%s == null", renamedAccountId), new FieldList(renamedAccountId))
                .retain(new FieldList(retainFields));
    }

    private Node renameFields(Node node, String oldName, String newName) {
        List<String> oldNames = node.getFieldNames();
        List<String> newNames = new ArrayList<>(oldNames);
        newNames.set(oldNames.indexOf(oldName), newName);
        return node.rename(new FieldList(oldNames), new FieldList(newNames));
    }
}
