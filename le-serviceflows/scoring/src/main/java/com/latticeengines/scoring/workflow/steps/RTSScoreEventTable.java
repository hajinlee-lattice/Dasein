package com.latticeengines.scoring.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;

@Component("rtsScoreEventTable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RTSScoreEventTable extends BaseRTSScoreStep<RTSScoreStepConfiguration> {

    public RTSScoreEventTable() {
    }

    @Override
    public void onConfigurationInitialized() {
        configuration.setRegisterScoredTable(true);
    }

}
