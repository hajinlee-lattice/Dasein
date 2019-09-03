package com.latticeengines.cdl.dataflow;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.UnmatchedAccountExportParameters;

@Component(UnmatchedAccountExportFlow.DATAFLOW_BEAN_NAME)
public class UnmatchedAccountExportFlow extends TypesafeDataFlowBuilder<UnmatchedAccountExportParameters> {
    public static final String DATAFLOW_BEAN_NAME = "unmatchedAccountExportFlow";

    @Override
    public Node construct(UnmatchedAccountExportParameters parameters) {
        Node srcAccount = addSource(parameters.getAccountTable());
        List<String> validatedColumns = parameters.getValidatedColumns();
        List<String> retainFields = srcAccount.getFieldNames().stream().filter(name -> validatedColumns.contains(name))
                .collect(Collectors.toList());
        String lattceId = InterfaceName.LatticeAccountId.name();
        srcAccount = srcAccount.filter(String.format("%s == null || %s == \"\"", lattceId, lattceId),
                new FieldList(lattceId)).retain(new FieldList(retainFields));
        return finalizeSchema(srcAccount);
    }

    private Node finalizeSchema(Node node) {
        if (node.getSchema(AccountId.name()) == null && node.getSchema(CustomerAccountId.name()) != null) {
            node = node.rename(new FieldList(CustomerAccountId.name()), new FieldList(AccountId.name()));
        }
        if (node.getSchema(ContactId.name()) == null && node.getSchema(CustomerContactId.name()) != null) {
            node = node.rename(new FieldList(CustomerContactId.name()), new FieldList(ContactId.name()));
        }
        return node;
    }
}
