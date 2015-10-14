package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.StratifiedSamplingMapper;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.StratifiedSamplingReducer;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class StratifiedSamplingJobCustomizer extends SamplingJobCustomizerBase {

    @Override
    protected void customizeMapper(Job job, SamplingConfiguration samplingConfig) {
        job.setMapperClass(StratifiedSamplingMapper.class);
    }

    @Override
    protected void customizePartitioner(Job job, SamplingConfiguration samplingConfig) {
    }

    @Override
    protected void customizeReducer(Job job, SamplingConfiguration samplingConfig) {
        job.setNumReduceTasks(1);
        job.setReducerClass(StratifiedSamplingReducer.class);
    }


}
