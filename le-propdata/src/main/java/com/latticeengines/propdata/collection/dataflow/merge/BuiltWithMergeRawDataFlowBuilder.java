package com.latticeengines.propdata.collection.dataflow.merge;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("builtWithMergeRawDataFlowBuilder")
@Scope("prototype")
public class BuiltWithMergeRawDataFlowBuilder extends MergeDomainBasedRawDataFlowBuilder {

    @Override
    protected String[] uniqueFields() {
        return new String[]{"Domain", "Technology_Name"};
    }

    @Override
    protected String timestampField() { return "LE_Last_Upload_Date"; }

    @Override
    protected String domainField() { return "Domain"; }
}
