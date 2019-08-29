package com.latticeengines.workflowapi.service;

import java.util.Set;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlerConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;

public interface WorkflowThrottlingService {

    WorkflowThrottlerConfiguration getThrottlingConfig(String podid, String division, Set<String> customerSpaces);

    WorkflowThrottlingSystemStatus constructSystemStatus(String podid, String division);

    boolean isWorkflowThrottlingEnabled(String podid, String division);
}
