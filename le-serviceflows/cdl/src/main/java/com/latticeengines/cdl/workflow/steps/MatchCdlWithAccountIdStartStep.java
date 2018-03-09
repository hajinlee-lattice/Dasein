package com.latticeengines.cdl.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithAccountIdStartStep")
public class MatchCdlWithAccountIdStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlWithAccountIdStartStep.class);

    @Autowired
    protected MetadataProxy metadataProxy;

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID, Table.class);
        if (inputTable == null) {
            log.info("There's no table with account Id, skip the workflow.");
            skipEmbeddedWorkflow(getParentNamespace(), "", MatchCdlAccountWorkflowConfiguration.class);
            return;
        }
        String path = inputTable.getExtracts().get(0).getPath();
        long count = AvroUtils.count(yarnConfiguration, path + "/" + "*.avro");
        if (count == 0) {
            log.info("There's no data with account Id, skip the workflow.");
            metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), inputTable.getName());
            removeObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID);
            skipEmbeddedWorkflow(getParentNamespace(), "", MatchCdlAccountWorkflowConfiguration.class);
            return;
        }

        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);
        putStringValueInContext(MATCH_FETCH_ONLY, "true");
        enableEmbeddedWorkflow(getParentNamespace(), "", MatchCdlAccountWorkflowConfiguration.class);

    }

}
