package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.DefaultSamplingMapper;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class DefaultSamplingJobCustomizer extends SamplingJobCustomizerBase {

    @Override
    protected void customizeMapper(Job job, SamplingConfiguration samplingConfig) {
        job.setMapperClass(DefaultSamplingMapper.class);
    }

}
