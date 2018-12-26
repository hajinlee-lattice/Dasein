package com.latticeengines.workflow.exposed.build;

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
    protected C workflowConf;

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
            if (workflowConf == null) {
                log.info("Skip the wrapped workflow steps.");
                skipEmbeddedWorkflow(getParentNamespace(), "", getWrappedWorkflowConfClass());
            } else {
                putObjectInContext(getParentNamespace(), workflowConf);
            }
            configuration.setPhase(POST_PROCESSING);
            putObjectInContext(namespace, configuration);
            log.info("In pre processing phase, skip on execution complete.");
        } else {
            log.info("In post processing phase.");
            if (workflowConf != null) {
                onPostProcessingCompleted();
            }
            configuration.setSkipStep(true);
            resetConfigurationPhase();
        }
    }

    @Override
    public void skipStep() {
        log.info("Skip the wrapper step and the wrapped workflow steps.");
        skipEmbeddedWorkflow(getParentNamespace(), "", getWrappedWorkflowConfClass());
        configuration.setSkipStep(true);
        resetConfigurationPhase();
    }

    private void resetConfigurationPhase() {
        BaseWrapperStepConfiguration stepConfig = getObjectFromContext(namespace, configuration.getClass());
        if (stepConfig != null) {
            log.info("Switch " + configuration.getClass().getName() + " to " + PRE_PROCESSING);
            stepConfig.setPhase(PRE_PROCESSING);
            putObjectInContext(namespace, stepConfig);
        }
    }

    protected void report(ReportPurpose purpose, String name, String json) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        report.setPurpose(purpose);
        report.setName(name);
        registerReport(configuration.getCustomerSpace(), report);
    }

    protected abstract Class<C> getWrappedWorkflowConfClass();

    protected abstract C executePreProcessing();

    protected abstract void onPostProcessingCompleted();
}
