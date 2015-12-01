package com.latticeengines.workflowapi.steps.template.upgrade;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class AbstractTemplateUpgradeStep extends AbstractStep<BaseStepConfiguration> {

    public abstract ApplicabilityEnum getApplicability();

}
