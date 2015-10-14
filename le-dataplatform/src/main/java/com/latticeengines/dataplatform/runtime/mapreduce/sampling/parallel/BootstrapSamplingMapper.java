package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.math3.distribution.BinomialDistribution;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.SamplingProperty;

public class BootstrapSamplingMapper extends RandomSamplingMapper {

    private BinomialDistribution binomial;

    @Override
    protected void setupSamplingTypeProperty(SamplingConfiguration sampleConfig) {
        int trainingDataSize = Integer.parseInt(sampleConfig.getProperty(SamplingProperty.TRAINING_DATA_SIZE
                .name()));
        int trainingSetSize = Integer.parseInt(sampleConfig.getProperty(SamplingProperty.TRAINING_SET_SIZE.name()));
        double probabilityOfSuccess = trainingSetSize / Math.pow(trainingDataSize, 2);
        binomial = new BinomialDistribution(trainingDataSize, probabilityOfSuccess);
    }

    @Override
    protected void createTrainingSample(AvroKey<Record> key, Context context) throws IOException, InterruptedException {
        for (SamplingElement element : trainingElements) {
            String trainingSetName = element.getName();
            int samplingTimes = binomial.sample();
            for (int i = 0; i < samplingTimes; i++) {
                outKey.set(trainingSetName);
                writeKeyToContext(context);
            }
        }
    }

}
