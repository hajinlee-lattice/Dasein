package com.latticeengines.cdl.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.UnmatchedAccountExportParameters;

@Component(UnmatchedAccountExportFlow.DATAFLOW_BEAN_NAME)
public class UnmatchedAccountExportFlow extends TypesafeDataFlowBuilder<UnmatchedAccountExportParameters> {
    public static final String DATAFLOW_BEAN_NAME = "unmatchedAccountExportFlow";

    @Override
    public Node construct(UnmatchedAccountExportParameters parameters) {
        Node srcAccount = addSource(parameters.getAccountTable());
        return srcAccount.filter(String.format("%s == null", InterfaceName.LatticeAccountId.name()),
                new FieldList(InterfaceName.LatticeAccountId.name()));
    }
}
