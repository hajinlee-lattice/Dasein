package com.latticeengines.scoring.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;

public interface ScoringStepYarnProcessor {

    ApplicationId executeYarnStep(String deploymentExternalId, ScoringCommandStep currentStep,
            ScoringCommand scoringCommand);

}
