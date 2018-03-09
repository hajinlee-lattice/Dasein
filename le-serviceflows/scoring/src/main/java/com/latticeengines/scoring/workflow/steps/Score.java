package com.latticeengines.scoring.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;

@Component("score")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Score extends BaseScoreStep<ScoreStepConfiguration> {
}
