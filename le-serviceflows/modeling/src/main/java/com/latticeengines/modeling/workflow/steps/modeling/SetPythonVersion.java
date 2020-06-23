package com.latticeengines.modeling.workflow.steps.modeling;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("setPythonVersion")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SetPythonVersion extends BaseWorkflowStep<ModelStepConfiguration> {

    @Value("${dataplatform.default.python.version}")
    private String pythonVersion;

    @Override
    public void execute() {
        // modeling will use default python version anyway
        // this is to mainly sync scoring python version
        putStringValueInContext(PYTHON_MAJOR_VERSION, pythonVersion);
    }

}
