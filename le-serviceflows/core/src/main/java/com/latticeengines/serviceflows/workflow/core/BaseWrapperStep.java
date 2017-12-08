package com.latticeengines.serviceflows.workflow.core;

import static com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase.POST_PROCESSING;
import static com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase.PRE_PROCESSING;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public abstract class BaseWrapperStep<T extends BaseWrapperStepConfiguration, C extends WorkflowConfiguration>
        extends BaseWorkflowStep<T> {

    private static Logger log = LoggerFactory.getLogger(BaseWrapperStep.class);
    private C workflowConf;

    @Override
    public void execute() {
        log.info("Execute " + getClass().getSimpleName() + " in phase " + configuration.getPhase());
        if (PRE_PROCESSING.equals(configuration.getPhase())) {
            workflowConf = executePreProcessing();
        } else {
            log.info("In post processing phase, skip execute.");
        }
    }

    @Override
    public void onExecutionCompleted() {
        if (PRE_PROCESSING.equals(configuration.getPhase())) {
            putObjectInContext(getWrappedWorkflowConfClass().getName(), workflowConf);
            String configClassName = configuration.getClass().getName();
            configuration.setPhase(POST_PROCESSING);
            putObjectInContext(configClassName, configuration);
            log.info("In pre processing phase, skip on execution complete.");
        } else {
            log.info("In post processing phase.");
            onPostProcessingCompleted();
            configuration.setSkipStep(true);
            resetConfigurationPhase();
        }
    }

    @Override
    public void skipStep() {
        log.info("Skip the wrapper step and the wrapped workflow steps.");
        skipEmbeddedWorkflow(getWrappedWorkflowConfClass());
        configuration.setSkipStep(true);
        resetConfigurationPhase();
    }

    private void resetConfigurationPhase() {
        BaseWrapperStepConfiguration stepConfig = getObjectFromContext(configuration.getClass().getName(),
                configuration.getClass());
        log.info("Switch " + configuration.getClass().getName() + " to " + PRE_PROCESSING);
        if (stepConfig != null) {
            stepConfig.setPhase(PRE_PROCESSING);
            putObjectInContext(configuration.getClass().getName(), stepConfig);
        }
    }

    protected void report(ReportPurpose purpose, String name, String json) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json.toString());
        report.setJson(kv);
        report.setPurpose(purpose);
        report.setName(name);
        registerReport(configuration.getCustomerSpace(), report);
    }

    protected abstract Class<C> getWrappedWorkflowConfClass();

    protected abstract C executePreProcessing();

    protected abstract void onPostProcessingCompleted();
}
