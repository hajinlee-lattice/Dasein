package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.integration.Broker;
import com.latticeengines.apps.cdl.integration.BrokerFactory;
import com.latticeengines.apps.cdl.service.InboundConnectionService;
import com.latticeengines.apps.cdl.workflow.BrokerInitialLoadWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.integration.BrokerInitialLoadRequest;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("inboundConnectionService")
public class InboundConnectionServiceImpl implements InboundConnectionService {

    private static final Logger log = LoggerFactory.getLogger(InboundConnectionServiceImpl.class);

    @Inject
    private BrokerFactory brokerFactory;

    @Inject
    private BrokerInitialLoadWorkflowSubmitter brokerInitialLoadWorkflowSubmitter;

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public BrokerReference setUpBroker(BrokerReference brokerReference) {
        return brokerFactory.setUpBroker(brokerReference);
    }

    @Override
    public Broker getBroker(BrokerReference brokerReference) {
        return brokerFactory.getBroker(brokerReference);
    }

    @Override
    public List<String> listDocumentTypes(BrokerReference brokerReference) {
        Broker broker = brokerFactory.getBroker(brokerReference);
        return broker.listDocumentTypes();
    }

    @Override
    public List<ColumnMetadata> describeDocumentType(BrokerReference brokerReference, String documentType) {
        Broker broker = brokerFactory.getBroker(brokerReference);
        return broker.describeDocumentType(documentType);
    }

    @Override
    public void submitMockBrokerAggregationWorkflow() {

    }

    @Override
    public void updateBroker(BrokerReference brokerReference) {
        Broker broker = brokerFactory.getBroker(brokerReference);
        broker.update(brokerReference);
    }

    @Override
    public BrokerReference getBrokerReference(BrokerReference brokerReference) {
        Broker broker = brokerFactory.getBroker(brokerReference);
        return broker.getBrokerReference();
    }

    private long getWorkflowPid(ApplicationId appId) {
        if (FakeApplicationId.isFakeApplicationId(appId.toString())) {
            return FakeApplicationId.toWorkflowJobPid(appId.toString());
        } else {
            Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString(), MultiTenantContext.getShortTenantId());
            return job.getPid();
        }
    }

    @Override
    public void schedule(BrokerReference brokerReference) {
        CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getShortTenantId());
        Broker broker = brokerFactory.getBroker(brokerReference);
        BrokerInitialLoadRequest brokerInitialLoadRequest = broker.schedule(brokerReference.getScheduler());
        ApplicationId appId = brokerInitialLoadWorkflowSubmitter.submit(customerSpace, brokerInitialLoadRequest, new WorkflowPidWrapper(-1l));
        long workflowPid = getWorkflowPid(appId);

    }
}
