package com.latticeengines.dataplatform.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public interface ParallelDispatchService {

    void customizeSampleConfig(SamplingConfiguration config);

    String getSampleJobName();

    String getModelingJobName();

    String getNumberOfSamplingTrainingSet();

    long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath) throws Exception;

    String getTrainingFile(String samplePrefix);

    String getTestFile(String samplePrefix);

    String getNumberOfProfilingMappers();

    String getProfileJobName();

    ApplicationId submitJob(ModelingJob modelingJob);

    Object getMapSizeKeyName();

}
