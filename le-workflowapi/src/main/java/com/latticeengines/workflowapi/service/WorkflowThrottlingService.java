package com.latticeengines.workflowapi.service;

import java.util.Set;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;

public interface WorkflowThrottlingService {

    WorkflowThrottlingConfiguration getThrottlingConfig(String podid, String division, Set<String> customerSpaces);

    WorkflowThrottlingSystemStatus constructSystemStatus(String podid, String division);

    boolean isWorkflowThrottlingEnabled(String podid, String division);

    boolean queueLimitReached(String customerSpace, String workflowType, String podid, String division);
}
