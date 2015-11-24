package com.latticeengines.workflowapi.steps.template.upgrade;

import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseStepConfiguration;

public abstract class AbstractTemplateUpgradeStep extends AbstractStep<BaseStepConfiguration> {

    public abstract ApplicabilityEnum getApplicability();

}
