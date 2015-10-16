package com.latticeengines.dataplatform.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public interface ParallelDispatchService {

    void customizeSampleConfig(SamplingConfiguration config, boolean isParallelEnabled);

    String getSampleJobName(boolean isParallelEnabled);

    String getModelingJobName(boolean isParallelEnabled);

    String getNumberOfSamplingTrainingSet(boolean isParallelEnabled);

    long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath, boolean isParallelEnabled) throws Exception;

    String getTrainingFile(String samplePrefix, boolean isParallelEnabled);

    String getTestFile(String samplePrefix, boolean isParallelEnabled);

    String getNumberOfProfilingMappers(boolean isParallelEnabled);

    String getProfileJobName(boolean isParallelEnabled);

    ApplicationId submitJob(ModelingJob modelingJob, boolean isParallelEnabled, boolean isModeling);

    String getMapSizeKeyName(boolean isParallelEnabled);

}
