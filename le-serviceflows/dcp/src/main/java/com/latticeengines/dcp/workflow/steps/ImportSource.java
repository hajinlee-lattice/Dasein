package com.latticeengines.dcp.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportSource extends BaseWorkflowStep<ImportSourceStepConfiguration> {

    @Override
    public void execute() {
        // TODO: import file using eai.

    }
}
