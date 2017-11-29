package com.latticeengines.workflow.exposed.build;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@StepScope
public abstract class AbstractStep<T> extends AbstractNameAwareBean {

    private static final Logger log = LoggerFactory.getLogger(AbstractStep.class);

    protected ExecutionContext executionContext;
    protected T configuration;
    protected Long jobId;

    private boolean dryRun = false;
    private boolean runAgainWhenComplete = true;
    private Class<T> configurationClass;
    private JobParameters jobParameters;

    public abstract void execute();

    @SuppressWarnings("unchecked")
    public AbstractStep() {
        this.configurationClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
    }

    public boolean setup() {
        T configuration = getObjectFromContext(configurationClass.getName(), configurationClass);
        if (configuration != null) {
            setConfiguration(configuration);
            return true;
        }
        String stepStringConfig = jobParameters.getString(configurationClass.getName());
        if (stepStringConfig != null) {
            setConfiguration(JsonUtils.deserialize(stepStringConfig, configurationClass));
            return true;
        }
        for (String key : jobParameters.getParameters().keySet()) {
            try {
                if (key.startsWith("com.") && configurationClass.isAssignableFrom(Class.forName(key))) {
                    stepStringConfig = jobParameters.getString(key);
                    if (stepStringConfig != null) {
                        setConfiguration(JsonUtils.deserialize(stepStringConfig, configurationClass));
                        return true;
                    }
                }
            } catch (ClassNotFoundException e) {
                log.error(ExceptionUtils.getStackTrace((e)));
                return false;
            }
        }
        return false;
    }

    public void setConfiguration(T configuration) {
        this.configuration = configuration;
        log.info("Configuration instance set for " + configurationClass.getName());

    }

    public void onConfigurationInitialized() {
    }

    public void onExecutionCompleted() {
    }

    public void skipStep() {
        log.info("Skipping step: " + configurationClass.getName());
    }

    /**
     * Override this to include any Step initialization logic.
     */
    @PostConstruct
    public void initialize() {
    }

    public T getConfiguration() {
        return configuration;
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

    public <V> V getConfigurationFromJobParameters(Class<V> configurationClass) {
        String stepStringConfig = jobParameters.getString(configurationClass.getName());
        return JsonUtils.deserialize(stepStringConfig, configurationClass);
    }

    public void setJobParameters(JobParameters jobParameters) {
        this.jobParameters = jobParameters;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    @SuppressWarnings("unchecked")
    protected String getOutputValue(String key) {
        Map<String, String> map = getObjectFromContext(WorkflowContextConstants.OUTPUTS, Map.class);
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected void putOutputValue(String key, String val) {
        Map<String, String> map = getObjectFromContext(WorkflowContextConstants.OUTPUTS, Map.class);
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(key, val);
        putObjectInContext(WorkflowContextConstants.OUTPUTS, map);
    }

    public <V> V getObjectFromContext(String key, Class<V> clazz) {
        String strValue = getStringValueFromContext(key);
        return JsonUtils.deserialize(strValue, clazz);
    }

    public <V> List<V> getListObjectFromContext(String key, Class<V> clazz) {
        List<?> list = getObjectFromContext(key, List.class);
        return JsonUtils.convertList(list, clazz);
    }

    public <K, V> Map<K, V> getMapObjectFromContext(String key, Class<K> keyClazz, Class<V> valueClazz) {
        Map<?, ?> map = getObjectFromContext(key, Map.class);
        return JsonUtils.convertMap(map, keyClazz, valueClazz);
    }

    protected <V> void putObjectInContext(String key, V val) {
        String json = JsonUtils.serialize(val);
        executionContext.putString(key, json);
        log.info("Updating " + key + " in context to " + json);
        // expand to its steps
        if (val instanceof WorkflowConfiguration) {
            log.info(val.getClass().getSimpleName() + " is a workflow configuration. Try to expand its steps.");
            WorkflowConfiguration workflowConfiguration = (WorkflowConfiguration) val;
            Map<String, Class<?>> stepConfigClasses = workflowConfiguration.getStepConfigClasses();
            if (!stepConfigClasses.isEmpty()) {
                Map<String, String> configRegistry = workflowConfiguration.getConfigRegistry();
                configRegistry.forEach((name, config) -> {
                    Class<?> stepConfigClass = stepConfigClasses.get(name);
                    putObjectInContext(name, JsonUtils.deserialize(config, stepConfigClass));
                });
            } else {
                log.warn("Trying to update workflow config for " + key + ". But cannot find its step config classes.");
            }
        }
    }

    protected void putStringValueInContext(String key, String val) {
        executionContext.put(key, val);
    }

    public String getStringValueFromContext(String key) {
        try {
            return executionContext.getString(key);
        } catch (ClassCastException e) {
            return null;
        }
    }

    public Double getDoubleValueFromContext(String key) {
        try {
            return executionContext.getDouble(key);
        } catch (ClassCastException e) {
            return null;
        }
    }

    protected void putDoubleValueInContext(String key, Double val) {
        executionContext.putDouble(key, val);
    }

    public Long getLongValueFromContext(String key) {
        try {
            return executionContext.getLong(key);
        } catch (ClassCastException e) {
            return null;
        }
    }

    protected void putLongValueInContext(String key, Long val) {
        executionContext.putLong(key, val);
    }

    public boolean shouldSkipStep() {
        return false;
    }
}
