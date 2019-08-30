package com.latticeengines.workflow.exposed.build;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.InjectableFailure;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.workflow.exposed.exception.InjectedWorkflowException;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;

@StepScope
public abstract class AbstractStep<T> extends AbstractNameAwareBean {

    private static final Logger log = LoggerFactory.getLogger(AbstractStep.class);

    protected ExecutionContext executionContext;
    protected T configuration;
    protected Long jobId;
    protected String namespace = "";
    protected JobParameters jobParameters;

    private boolean dryRun = false;
    private boolean runAgainWhenComplete = true;
    private Class<T> configurationClass;

    private int seq;
    private InjectableFailure injectedFailure;

    public abstract void execute();

    @SuppressWarnings("unchecked")
    public AbstractStep() {
        this.configurationClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
    }

    public boolean setup() {
        T configuration = getObjectFromContext(namespace, configurationClass);
        if (configuration == null) {
            String stepStringConfig = jobParameters.getString(namespace);
            if (StringUtils.isNotEmpty(stepStringConfig)) {
                configuration = JsonUtils.deserialize(stepStringConfig, configurationClass);
            }
        }
        if (configuration != null) {
            setConfiguration(configuration);
            return true;
        }
        return false;
    }

    public void setConfiguration(T configuration) {
        this.configuration = configuration;
        log.info("Configuration instance {} set for {}", configuration, configurationClass.getName());
    }

    public void onConfigurationInitialized() {
    }

    public void onExecutionCompleted() {
    }

    public void skipStep() {
        log.info("Skipping step: " + getClass().getSimpleName());
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

    public BaseStepConfiguration getConfigurationFromJobParameters(String namespace) {
        String stepStringConfig = jobParameters.getString(namespace);
        return JsonUtils.deserialize(stepStringConfig, BaseStepConfiguration.class);
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

    public <V> Set<V> getSetObjectFromContext(String key, Class<V> clazz) {
        Set<?> set = getObjectFromContext(key, Set.class);
        return JsonUtils.convertSet(set, clazz);
    }

    public <K, V> Map<K, V> getMapObjectFromContext(String key, Class<K> keyClazz, Class<V> valueClazz) {
        Map<?, ?> map = getObjectFromContext(key, Map.class);
        return JsonUtils.convertMap(map, keyClazz, valueClazz);
    }

    public <V> void putObjectInContext(String key, V val) {
        String json = JsonUtils.serialize(val);
        executionContext.putString(key, json);
        log.debug("Updating " + key + " in context to " + json);
        // expand to its steps
        if (val instanceof WorkflowConfiguration) {
            log.info(val.getClass().getSimpleName() + " is a workflow configuration. Try to expand its steps.");
            WorkflowConfiguration workflowConfig = (WorkflowConfiguration) val;
            Map<String, String> configRegistry = WorkflowUtils.getFlattenedConfig(workflowConfig);
            String parentNamespace = key;
            configRegistry.forEach((name, config) -> {
                putObjectInContext(String.format("%s.%s", parentNamespace, name),
                        JsonUtils.deserialize(config, BaseStepConfiguration.class));
            });
        }

    }

    public <V> void addToListInContext(String key, V object, Class<V> clz) {
        List<V> list = getListObjectFromContext(key, clz);
        if (list == null) {
            list = new ArrayList<>();
        } else {
            list = new ArrayList<>(list);
        }
        list.add(object);
        putObjectInContext(key, list);
    }

    protected void clearExecutionContext(Set<String> retryableKeys) {
        Map<String, Object> retryableContext = new HashMap<>();
        if (CollectionUtils.isNotEmpty(retryableKeys)) {
            log.info(("There are " + executionContext.entrySet().size() + " keys in execution context."));
            executionContext.entrySet().forEach(entry -> {
                if (retryableKeys.contains(entry.getKey())) {
                    log.info(String.format("Putting %s: %s", entry.getKey(), entry.getValue()));
                    retryableContext.put(entry.getKey(), entry.getValue());
                }
            });
        }
        executionContext.entrySet().clear();
        if (MapUtils.isNotEmpty(retryableContext)) {
            retryableContext.forEach(executionContext::put);
        }
    }

    public boolean hasKeyInContext(String key) {
        return executionContext.containsKey(key);
    }

    protected void removeObjectFromContext(String key) {
        log.info("Removing " + key + " from context.");
        executionContext.remove(key);
    }

    protected void putStringValueInContext(String key, String val) {
        log.info("Updating " + key + " in context to " + val);
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

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getParentNamespace() {
        return namespace.substring(0, namespace.lastIndexOf('.'));
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public InjectableFailure getInjectedFailure() {
        return injectedFailure;
    }

    public void setInjectedFailure(InjectableFailure injectedFailure) {
        this.injectedFailure = injectedFailure;
    }

    public void throwFailureIfInjected(InjectableFailure failure) {
        if (failure != null && failure.equals(injectedFailure)) {
            throw new InjectedWorkflowException(
                    String.format("Throw injected failure %s at [%d] %s", failure.name(), getSeq(), name()));
        }
    }

}
