package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingProperty;

public class DownSamplingMapper extends RandomSamplingMapper {
    private String targetColumnName;
    private String majorityClassLabel;
    private int downToPercentage;

    @Override
    protected void setupSamplingTypeProperty(SamplingConfiguration sampleConfig) {
        targetColumnName = sampleConfig.getProperty(SamplingProperty.TARGET_COLUMN_NAME.name());
        majorityClassLabel = sampleConfig.getProperty(SamplingProperty.MAJORITY_CLASS_LABEL.name());
        downToPercentage = Integer.parseInt(sampleConfig.getProperty(SamplingProperty.DOWN_TO_PERCENTAGE.name()));
    }

    protected void createTrainingSample(AvroKey<Record> key, Context context) throws IOException, InterruptedException {
        Record record = key.datum();
        String classLabel = record.get(targetColumnName).toString();
        if (classLabel.equals(majorityClassLabel)) {
            int removeOrNot = random.nextInt(100);
            if (removeOrNot < downToPercentage) {
                writeTrainingSample(context);
            }
        } else {
            writeTrainingSample(context);
        }
    }

}
