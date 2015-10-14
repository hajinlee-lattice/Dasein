package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.EventDataSamplingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public abstract class RandomSamplingMapper extends Mapper<AvroKey<Record>, NullWritable, Text, AvroValue<Record>> {
    protected List<SamplingElement> trainingElements;
    protected SamplingElement testingElement;
    protected int samplingRate;
    protected int trainingPercentage;
    protected int trainingSetCount;

    protected Text outKey;
    protected AvroValue<Record> outValue;
    protected Random random;

    @Override
    protected void setup(Context context) {
        Configuration config = context.getConfiguration();
        String sampleConfigStr = config.get(EventDataSamplingJob.LEDP_SAMPLE_CONFIG);
        SamplingConfiguration sampleConfig = JsonUtils.deserialize(sampleConfigStr, SamplingConfiguration.class);

        setupCommonSamplingProperty(sampleConfig);
        setupSamplingTypeProperty(sampleConfig);

        outKey = new Text();
        outValue = new AvroValue<Record>();
        random = new Random();
    }

    private void setupCommonSamplingProperty(SamplingConfiguration sampleConfig) {
        trainingElements = sampleConfig.getTrainingElements();
        testingElement = sampleConfig.getTestingElement();
        trainingPercentage = sampleConfig.getTrainingPercentage();
        trainingSetCount = sampleConfig.getTrainingSetCount();
        samplingRate = sampleConfig.getSamplingRate();
    }

    protected abstract void setupSamplingTypeProperty(SamplingConfiguration sampleConfig);

    @Override
    protected void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,
            InterruptedException {
        int randomNumber = random.nextInt(100);
        if (randomNumber < samplingRate) {
            createSample(key, context);
        }
    }

    protected void createSample(AvroKey<Record> key, Context context) throws IOException, InterruptedException {
        outValue.datum(key.datum());
        int trainingOrTest = random.nextInt(100);
        if (trainingOrTest > trainingPercentage) {
            String testingSetName = testingElement.getName();
            outKey.set(testingSetName);
            context.write(outKey, outValue);
        } else {
            createTrainingSample(key, context);
        }
    }

    protected abstract void createTrainingSample(AvroKey<Record> key, Context context) throws IOException,
            InterruptedException;

    protected void writeTrainingSample(Context context) throws IOException, InterruptedException {
        int trainingSetIndex = random.nextInt(trainingSetCount);
        String trainingSetName = trainingElements.get(trainingSetIndex).getName();
        outKey.set(trainingSetName);
        writeKeyToContext(context);
    }

    protected void writeKeyToContext(Context context) throws IOException, InterruptedException {
        context.write(outKey, outValue);
        outKey.set(SamplingConfiguration.TRAINING_ALL_PREFIX);
        context.write(outKey, outValue);
    }
}
