package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("playLaunchWorkflowSubmitter")
public class PlayLaunchWorkflowSubmitter extends WorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowSubmitter.class);

    @Inject
    private BatonService batonService;

    public ApplicationId submit(PlayLaunch playLaunch) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");

        boolean enableExport = batonService.isEnabled(getCustomerSpace(), LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION);
        boolean canBeLaunchedToExternal = enableExport && isValidDestination(playLaunch.getDestinationSysType());

        PlayLaunchWorkflowConfiguration configuration = new PlayLaunchWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow("playLaunchWorkflow") //
                .inputProperties(inputProperties) //
                .playLaunch(playLaunch) //
                .exportPlayLaunch(playLaunch, canBeLaunchedToExternal)
                .build();
        return workflowJobService.submit(configuration);
    }

    private boolean isValidDestination(CDLExternalSystemType destinationSysType) {
        switch (destinationSysType) {
        case MAP:
            return true;
        default:
            return false;
        }
    }
}
