package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.scoring.BaseRTSScoreStep;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RTSScoreStepConfiguration;

@Component("rtsScoreEventTable")
public class RTSScoreEventTable extends BaseRTSScoreStep<RTSScoreStepConfiguration> {

    public RTSScoreEventTable() {
    }

    @Override
    public void onConfigurationInitialized() {
        configuration.setRegisterScoredTable(true);
    }

}
