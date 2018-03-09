package com.latticeengines.cdl.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlLdcStartStep")
public class MatchCdlLdcStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlLdcStartStep.class);

    @Autowired
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
