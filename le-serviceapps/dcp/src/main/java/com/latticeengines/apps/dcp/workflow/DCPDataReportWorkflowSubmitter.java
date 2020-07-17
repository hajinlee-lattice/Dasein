package com.latticeengines.apps.dcp.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPDataReportWorkflowConfiguration;

@Component("dcpDataReportWorkflowSubmitter")
public class DCPDataReportWorkflowSubmitter extends WorkflowSubmitter {

    @Value("${yarn.pls.url}")
    protected String internalResourceHostPort;

    @Value("${common.microservice.url}")
    protected String microserviceHostPort;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, DCPReportRequest reportRequest,
                                WorkflowPidWrapper pidWrapper) {

        DCPDataReportWorkflowConfiguration configuration =
                generateConfiguration(customerSpace, reportRequest.getRootId(), reportRequest.getLevel(),
                        reportRequest.getMode());
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        return applicationId;
    }

    private DCPDataReportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
                                                                     String rootId,
                                                                     DataReportRecord.Level level,
                                                                     DataReportMode mode) {
        return new DCPDataReportWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .internalResourceHostPort(internalResourceHostPort)
                .microServiceHostPort(microserviceHostPort)
                .rootId(rootId)
                .level(level)
                .mode(mode)
                .builder();
    }


}
