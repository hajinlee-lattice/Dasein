package com.latticeengines.scoring.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;

@Component("rtsScoreEventTable")
public class RTSScoreEventTable extends BaseRTSScoreStep<RTSScoreStepConfiguration> {

    public RTSScoreEventTable() {
    }

    @Override
    public void onConfigurationInitialized() {
        configuration.setRegisterScoredTable(true);
    }

}
