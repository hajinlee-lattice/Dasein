package com.latticeengines.propdata.collection.dataflow.merge;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("featureMergeRawDataFlowBuilder")
@Scope("prototype")
public class FeatureMergeRawDataFlowBuilder extends MergeDomainBasedRawDataFlowBuilder {

    @Override
    protected String[] uniqueFields() { return new String[]{"URL", "Feature"}; }

    @Override
    protected String timestampField() { return "LE_Last_Upload_Date"; }

    @Override
    protected String domainField() { return "URL"; }

}
