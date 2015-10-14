package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import org.apache.hadoop.mapreduce.Job;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.UpSamplingMapper;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class UpSamplingJobCustomizer extends SamplingJobCustomizerBase {

    @Override
    protected void customizeMapper(Job job, SamplingConfiguration samplingConfig) {
        job.setMapperClass(UpSamplingMapper.class);
    }

}
