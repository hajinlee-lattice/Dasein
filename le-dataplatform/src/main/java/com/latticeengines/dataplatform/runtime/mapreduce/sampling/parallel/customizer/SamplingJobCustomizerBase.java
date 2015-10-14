package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.DefaultSamplingPartitioner;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.DefaultSamplingReducer;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public abstract class SamplingJobCustomizerBase implements SamplingJobCustomizer {

    @Override
    public void customizeJob(Job job, SamplingConfiguration samplingConfig) {
        customizeMapper(job, samplingConfig);
        customizePartitioner(job, samplingConfig);
        customizeReducer(job, samplingConfig);
    }

    protected abstract void customizeMapper(Job job, SamplingConfiguration samplingConfig);

    protected void customizePartitioner(Job job, SamplingConfiguration samplingConfig) {
        job.setPartitionerClass(DefaultSamplingPartitioner.class);
    }

    protected void customizeReducer(Job job, SamplingConfiguration samplingConfig) {
        job.setNumReduceTasks(2);
        job.setReducerClass(DefaultSamplingReducer.class);
    }
}
