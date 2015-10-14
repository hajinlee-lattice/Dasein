package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class DefaultSamplingMapper extends RandomSamplingMapper {

    protected void setupSamplingTypeProperty(SamplingConfiguration sampleConfig) {
    };

    protected void createTrainingSample(AvroKey<Record> key, Context context) throws IOException, InterruptedException {
        writeTrainingSample(context);
    }
}
