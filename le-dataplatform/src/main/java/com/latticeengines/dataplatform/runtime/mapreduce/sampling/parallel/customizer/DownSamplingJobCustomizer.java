package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.DownSamplingMapper;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class DownSamplingJobCustomizer extends SamplingJobCustomizerBase {

    @Override
    protected void customizeMapper(Job job, SamplingConfiguration samplingConfig) {
        job.setMapperClass(DownSamplingMapper.class);
    }

}
