package com.latticeengines.dataplatform.service.impl;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.runtime.python.PythonMRJobType;
import com.latticeengines.dataplatform.service.ParallelDispatchService;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Component("ParallelDispatchService")
public class ParallelDispatchServiceImpl implements ParallelDispatchService {

    @Value("${dataplatform.model.parallel.enabled:false}")
    private boolean parallelEnabled;

    @Resource(name = "SingleContainerDispatcher")
    private ParallelDispatchService singleContainerDispatcher;

    @Resource(name = "MutipleContainerDispatcher")
    private ParallelDispatchService multipleContainerDispatcher;

    private ParallelDispatchService defaultContainerDispatcher;

    @PostConstruct
    public void init() {
        if (parallelEnabled) {
            defaultContainerDispatcher = multipleContainerDispatcher;
        } else {
            defaultContainerDispatcher = singleContainerDispatcher;
        }
    }

    @Override
    public void customizeSampleConfig(SamplingConfiguration config) {
        getContainerDispatcher().customizeSampleConfig(config);
    }

    @Override
    public String getSampleJobName() {
        return getContainerDispatcher().getSampleJobName();
    }

    @Override
    public String getModelingJobName() {
        return PythonMRJobType.MODELING_JOB.jobType();
    }

    @Override
    public String getNumberOfSamplingTrainingSet() {
        return getContainerDispatcher().getNumberOfSamplingTrainingSet();
    }

    @Override
    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath) throws Exception {
        return getContainerDispatcher().getSampleSize(yarnConfiguration, diagnosticsPath);
    }

    @Override
    public String getTrainingFile(String samplePrefix) {
        return getContainerDispatcher().getTrainingFile(samplePrefix);
    }

    @Override
    public String getTestFile(String samplePrefix) {
        return getContainerDispatcher().getTestFile(samplePrefix);
    }

    @Override
    public String getNumberOfProfilingMappers() {
        return getContainerDispatcher().getNumberOfProfilingMappers();
    }

    @Override
    public String getProfileJobName() {
        return getContainerDispatcher().getProfileJobName();
    }

    private ParallelDispatchService getContainerDispatcher() {
        return defaultContainerDispatcher;
    }

    @Override
    public ApplicationId submitJob(ModelingJob modelingJob) {
        return getContainerDispatcher().submitJob(modelingJob);
    }

    @Override
    public Object getMapSizeKeyName() {
        return getContainerDispatcher().getMapSizeKeyName();
    }
}
