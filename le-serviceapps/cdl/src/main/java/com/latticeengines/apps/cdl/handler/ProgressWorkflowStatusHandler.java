package com.latticeengines.apps.cdl.handler;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;

@Component
public class ProgressWorkflowStatusHandler implements WorkflowStatusHandler {

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Progress;
    }

}
