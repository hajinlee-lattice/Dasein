package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.math3.distribution.BinomialDistribution;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.SamplingProperty;

public class UpSamplingMapper extends RandomSamplingMapper {
    private String targetColumnName;
    private String minorityClassLabel;
    private BinomialDistribution binomial;

    @Override
    protected void setupSamplingTypeProperty(SamplingConfiguration sampleConfig) {
        targetColumnName = sampleConfig.getProperty(SamplingProperty.TARGET_COLUMN_NAME.name());
        minorityClassLabel = sampleConfig.getProperty(SamplingProperty.MINORITY_CLASS_LABEL.name());
        int minorityClassSize = Integer.parseInt(sampleConfig.getProperty(SamplingProperty.MINORITY_CLASS_SIZE.name()));
        int upToPercentage = Integer.parseInt(sampleConfig.getProperty(SamplingProperty.UP_TO_PERCENTAGE.name()));

        int trainingMinorityClassSize = getIntegerSizeFromPercentage(minorityClassSize, trainingPercentage);
        int trainingSetMinorityClassSize = Math.round((float) trainingMinorityClassSize / trainingSetCount);
        int upToTrainingSetMinorityClassSize = getIntegerSizeFromPercentage(trainingSetMinorityClassSize,
                upToPercentage);

        double probabilityOfSuccess = upToTrainingSetMinorityClassSize / Math.pow(trainingMinorityClassSize, 2);
        binomial = new BinomialDistribution(trainingMinorityClassSize, probabilityOfSuccess);
    }

    private int getIntegerSizeFromPercentage(int totalSize, int percentage) {
        return Math.round(totalSize * ((float) percentage / 100));
    }

    protected void createTrainingSample(AvroKey<Record> key, Context context) throws IOException, InterruptedException {
        Record record = key.datum();
        String classLabel = record.get(targetColumnName).toString();
        if (classLabel.equals(minorityClassLabel)) {

            for (SamplingElement element : trainingElements) {
                String trainingSetName = element.getName();
                int samplingTimes = binomial.sample();
                for (int i = 0; i < samplingTimes; i++) {
                    outKey.set(trainingSetName);
                    writeKeyToContext(context);
                }
            }
        } else {
            writeTrainingSample(context);
        }
    }

}
