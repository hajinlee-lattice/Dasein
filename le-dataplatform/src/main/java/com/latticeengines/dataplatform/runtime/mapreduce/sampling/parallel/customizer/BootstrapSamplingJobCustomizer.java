package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.BootstrapSamplingMapper;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class BootstrapSamplingJobCustomizer extends SamplingJobCustomizerBase {

    @Override
    protected void customizeMapper(Job job, SamplingConfiguration samplingConfig) {
        job.setMapperClass(BootstrapSamplingMapper.class);
    }

}
