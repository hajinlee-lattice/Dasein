package com.latticeengines.dataplatform.service.impl;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.DispatchService;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Component("parallelDispatchService")
public class ParallelDispatchServiceImpl implements DispatchService {

    private static final Logger log = LoggerFactory.getLogger(ParallelDispatchServiceImpl.class);

    @Value("${dataplatform.model.parallel.enabled:false}")
    private boolean configParallelEnabled;

    @Value("${dataplatform.model.aws.batch.enabled:false}")
    private boolean modelAwsBatchEnabled;

    @Resource(name = "singleContainerDispatcher")
    private DispatchService singleContainerDispatcher;

    @Resource(name = "awsBatchContainerDispatcher")
    private DispatchService awsBatchContainerDispatcher;

    @Resource(name = "mutipleContainerDispatcher")
    private DispatchService multipleContainerDispatcher;

    @Value("${hadoop.use.emr:false}")
    private Boolean useEmr;

    private DispatchService defaultContainerDispatcher;

    @PostConstruct
    public void init() {
        if (configParallelEnabled) {
            defaultContainerDispatcher = multipleContainerDispatcher;
        } else {
            defaultContainerDispatcher = singleContainerDispatcher;
        }
    }

    @Override
    public void customizeSampleConfig(SamplingConfiguration config, boolean isParallelEnabled) {
        getContainerDispatcher(isParallelEnabled).customizeSampleConfig(config, isParallelEnabled);
    }

    @Override
    public void customizeEventCounterConfig(EventCounterConfiguration config, boolean isParallelEnabled) {
        getContainerDispatcher(isParallelEnabled).customizeEventCounterConfig(config, isParallelEnabled);
    }

    @Override
    public String getSampleJobName(boolean isParallelEnabled) {
        return getContainerDispatcher(isParallelEnabled).getSampleJobName(isParallelEnabled);
    }

    @Override
    public String getEventCounterJobName(boolean isParallelEnabled) {
        return getContainerDispatcher(isParallelEnabled).getEventCounterJobName(isParallelEnabled);
    }

    @Override
    public String getModelingJobName(boolean isParallelEnabled) {
        if (configParallelEnabled && !isParallelEnabled) {
            return singleContainerDispatcher.getModelingJobName(isParallelEnabled);
        }
        return getContainerDispatcher(isParallelEnabled).getModelingJobName(isParallelEnabled);
    }

    @Override
    public String getNumberOfSamplingTrainingSet(boolean isParallelEnabled) {
        if (configParallelEnabled && !isParallelEnabled) {
            return singleContainerDispatcher.getNumberOfProfilingMappers(isParallelEnabled);
        }
        return getContainerDispatcher(isParallelEnabled).getNumberOfSamplingTrainingSet(isParallelEnabled);
    }

    @Override
    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath, boolean isParallelEnabled)
            throws Exception {
        return getContainerDispatcher(isParallelEnabled).getSampleSize(yarnConfiguration, diagnosticsPath,
                isParallelEnabled);
    }

    @Override
    public String getTrainingFile(String samplePrefix, boolean isParallelEnabled) {
        return getContainerDispatcher(isParallelEnabled).getTrainingFile(samplePrefix, isParallelEnabled);
    }

    @Override
    public String getTestFile(String samplePrefix, boolean isParallelEnabled) {
        return getContainerDispatcher(isParallelEnabled).getTestFile(samplePrefix, isParallelEnabled);
    }

    @Override
    public String getNumberOfProfilingMappers(boolean isParallelEnabled) {
        return getContainerDispatcher(isParallelEnabled).getNumberOfProfilingMappers(isParallelEnabled);
    }

    @Override
    public String getProfileJobName(boolean isParallelEnabled) {
        return getContainerDispatcher(isParallelEnabled).getProfileJobName(isParallelEnabled);
    }

    @Override
    public ApplicationId submitJob(ModelingJob modelingJob, boolean isParallelEnabled, boolean isModeling) {

        log.info("useEmr=" + useEmr + " modelAwsBatchEnabled=" + modelAwsBatchEnabled + "isParallelEnabled="
                + isParallelEnabled);
        if (modelAwsBatchEnabled && !isParallelEnabled) {
            return awsBatchContainerDispatcher.submitJob(modelingJob, isParallelEnabled, isModeling);
        }
        if (configParallelEnabled && !isParallelEnabled && isModeling) {
            return singleContainerDispatcher.submitJob(modelingJob, isParallelEnabled, isModeling);
        }
        return getContainerDispatcher(isParallelEnabled).submitJob(modelingJob, isParallelEnabled, isModeling);
    }

    @Override
    public String getMapSizeKeyName(boolean isParallelEnabled) {
        if (configParallelEnabled && !isParallelEnabled) {
            return singleContainerDispatcher.getMapSizeKeyName(isParallelEnabled);
        }
        return getContainerDispatcher(isParallelEnabled).getMapSizeKeyName(isParallelEnabled);
    }

    private DispatchService getContainerDispatcher(boolean isParallelEnabled) {
        if (isParallelEnabled) {
            return multipleContainerDispatcher;
        }

        return defaultContainerDispatcher;
    }
}
