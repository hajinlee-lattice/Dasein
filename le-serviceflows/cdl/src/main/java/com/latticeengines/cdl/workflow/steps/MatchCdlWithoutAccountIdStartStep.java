package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.MatchCdlWithoutAccountIdWorkflow;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithoutAccountIdStartStep")
public class MatchCdlWithoutAccountIdStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlWithoutAccountIdStartStep.class);

    @Inject
    private MatchCdlWithoutAccountIdWorkflow noAccountIdWorkflow;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, Table.class);
        if (inputTable == null) {
            log.info("There's no table without account Id, skip the workflow.");
            skipEmbeddedWorkflow(getParentNamespace(), noAccountIdWorkflow.name(),
                    MatchCdlAccountWorkflowConfiguration.class);
            return;
        }
        String path = inputTable.getExtracts().get(0).getPath();
        long count = 0;
        path = PathUtils.toAvroGlob(path);
        count = AvroUtils.count(yarnConfiguration, path);
        if (count == 0) {
            log.info("There's no data without account Id, skip the workflow.");
            metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), inputTable.getName());
            removeObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID);
            skipEmbeddedWorkflow(getParentNamespace(), noAccountIdWorkflow.name(),
                    MatchCdlAccountWorkflowConfiguration.class);
            return;
        }

        enableEmbeddedWorkflow(getParentNamespace(), noAccountIdWorkflow.name(),
                MatchCdlAccountWorkflowConfiguration.class);
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);

    }

}
