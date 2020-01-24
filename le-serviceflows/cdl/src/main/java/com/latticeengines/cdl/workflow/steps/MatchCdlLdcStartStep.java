package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlLdcStartStep")
public class MatchCdlLdcStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Inject
    protected MetadataProxy metadataProxy;

    @Override
    public void execute() {
        Table inputTable = getInputTable();
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);
    }

    private Table getInputTable() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_IMPORT, Table.class);
        if (inputTable != null)
            return inputTable;
        return metadataProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                configuration.getMatchInputTableName());
    }

}
