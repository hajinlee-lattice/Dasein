package com.latticeengines.apps.cdl.integration;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.apps.cdl.workflow.BrokerInitialLoadWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.integration.BrokerInitialLoadRequest;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public abstract class BaseBroker implements Broker {

    protected String sourceId;

    @Inject
    protected BrokerInitialLoadWorkflowSubmitter brokerInitialLoadWorkflowSubmitter;

    @Inject
    protected WorkflowProxy workflowProxy;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    protected BaseBroker(BrokerReference brokerReference) {
        this.sourceId = brokerReference.getSourceId();
    }

    private long getWorkflowPid(ApplicationId appId) {
        if (FakeApplicationId.isFakeApplicationId(appId.toString())) {
            return FakeApplicationId.toWorkflowJobPid(appId.toString());
        } else {
            Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString(), MultiTenantContext.getShortTenantId());
            return job.getPid();
        }
    }

    protected long submitInitialLoadWorkflow(CustomerSpace customerSpace, BrokerInitialLoadRequest brokerInitialLoadRequest) {
        ApplicationId appId = brokerInitialLoadWorkflowSubmitter.submit(customerSpace, brokerInitialLoadRequest, new WorkflowPidWrapper(-1l));
        return getWorkflowPid(appId);

    }

}
