package com.latticeengines.scoring.workflow.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ScoreStepConfiguration;
import org.springframework.stereotype.Component;

@Component("score")
public class Score extends BaseScoreStep<ScoreStepConfiguration> {
}
