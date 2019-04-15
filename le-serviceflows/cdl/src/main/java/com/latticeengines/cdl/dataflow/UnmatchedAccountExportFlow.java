package com.latticeengines.cdl.dataflow;

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
        return srcAccount.filter(String.format("%s == null || %s == \"\"", lattceId, lattceId),
                new FieldList(lattceId)).retain(new FieldList(retainFields));
    }
}
