package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startProfile")
public class StartProfile extends BaseWorkflowStep<CalculateStatsStepConfiguration> {

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void execute() {
        DataFeedProfile profile = dataFeedProxy.updateProfileWorkflowId(configuration.getCustomerSpace().toString(),
                jobId);
        if (profile == null) {
            throw new RuntimeException("profile is null!!");
        } else if (profile.getWorkflowId().longValue() != jobId.longValue()) {
            throw new RuntimeException(
                    String.format("current active profile has a workflow id %s, which is different from %s ",
                            profile.getWorkflowId(), jobId));
        }
    }

}
