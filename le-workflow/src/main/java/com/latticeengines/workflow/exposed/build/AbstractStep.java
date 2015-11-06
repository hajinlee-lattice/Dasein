package com.latticeengines.workflow.exposed.build;

import java.lang.reflect.ParameterizedType;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;

import com.latticeengines.common.exposed.util.JsonUtils;

@StepScope
public abstract class AbstractStep<T> extends AbstractNameAwareBean {

    private static final Log log = LogFactory.getLog(AbstractStep.class);

    protected ExecutionContext executionContext;
    protected T configuration;

    private boolean dryRun = false;
    private boolean runAgainWhenComplete = false;
    private Class<T> configurationClass;
    private JobParameters jobParameters;

    public abstract void execute();

    @SuppressWarnings("unchecked")
    public AbstractStep() {
        this.configurationClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public void setup() {
        String stepStringConfig = jobParameters.getString(configurationClass.getName());
        if (stepStringConfig != null) {
            configuration = JsonUtils.deserialize(stepStringConfig, configurationClass);
            log.info("Configuration instance set for " + configurationClass.getName());
        }
    }

    /**
     * Override this to include any Step initialization logic.
     */
    @PostConstruct
    public void initialize() {
    }

    public Class<T> getConfigurationClass() {
        return configurationClass;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public boolean isRunAgainWhenComplete() {
        return runAgainWhenComplete;
    }

    public void setRunAgainWhenComplete(boolean runAgainWhenComplete) {
        this.runAgainWhenComplete = runAgainWhenComplete;
    }

    public void setJobParameters(JobParameters jobParameters) {
        this.jobParameters = jobParameters;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }
}
