package com.latticeengines.workflow.exposed.build;

import java.lang.reflect.ParameterizedType;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.core.WorkflowTranslator;

public abstract class AbstractWorkflow<T extends WorkflowConfiguration> extends AbstractNameAwareBean {

    public abstract Workflow defineWorkflow(T workflowConfig);

    @SuppressWarnings("unchecked")
    public Class<T> getWorkflowConfigurationType() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Autowired
    private WorkflowTranslator workflowTranslator;

    public Job buildWorkflow(T config) throws Exception {
        Workflow workflow = defineWorkflow(config);
        return workflowTranslator.buildWorkflow(name(), workflow);
    }

}
