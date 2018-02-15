package com.latticeengines.scoring.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;

@Component("score")
public class Score extends BaseScoreStep<ScoreStepConfiguration> {
}
