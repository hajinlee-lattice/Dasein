package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public interface SamplingJobCustomizer {
    public void customizeJob(Job job, SamplingConfiguration samplingConfig);
}
