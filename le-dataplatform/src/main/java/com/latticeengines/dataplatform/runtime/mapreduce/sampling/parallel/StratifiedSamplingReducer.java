package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.EventDataSamplingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public class StratifiedSamplingReducer extends Reducer<Text, AvroValue<Record>, AvroKey<Record>, NullWritable> {
    private List<SamplingElement> trainingElements;
    private SamplingElement testingElement;
    private int samplingRate;
    private int trainingPercentage;
    private int trainingSetCount;

    private Map<String, Integer> classLabelToClassIndex;
    private int[] sampleSize;
    private int[] trainingSize;
    private int[] sampleCount;

    private AvroMultipleOutputs outputs;
    private AvroKey<Record> outKey;
    private static final NullWritable nullWritable = NullWritable.get();

    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        String sampleConfigStr = config.get(EventDataSamplingJob.LEDP_SAMPLE_CONFIG);
        SamplingConfiguration sampleConfig = JsonUtils.deserialize(sampleConfigStr, SamplingConfiguration.class);
        setupCommonSamplingProperty(sampleConfig);
        setupSamplingTypeProperty(sampleConfig);

        outputs = new AvroMultipleOutputs(context);
        outKey = new AvroKey<Record>();
    }

    private void setupCommonSamplingProperty(SamplingConfiguration sampleConfig) {
        trainingElements = sampleConfig.getTrainingElements();
        testingElement = sampleConfig.getTestingElement();
        trainingPercentage = sampleConfig.getTrainingPercentage();
        trainingSetCount = sampleConfig.getTrainingSetCount();
        samplingRate = sampleConfig.getSamplingRate();
    }

    private void setupSamplingTypeProperty(SamplingConfiguration sampleConfig) {
        Map<String, Long> counterGroupResultMap = sampleConfig.getCounterGroupResultMap();
        classLabelToClassIndex = new HashMap<String, Integer>();
        sampleSize = new int[counterGroupResultMap.size()];
        trainingSize = new int[counterGroupResultMap.size()];
        sampleCount = new int[counterGroupResultMap.size()];

        Iterator<String> iterator = counterGroupResultMap.keySet().iterator();
        int i = 0;

        while (iterator.hasNext()) {
            String classLabel = iterator.next();
            Long classSize = counterGroupResultMap.get(classLabel);
            classLabelToClassIndex.put(classLabel, i);
            sampleSize[i] = getIntegerSizeFromPercentage(classSize.intValue(), samplingRate);
            trainingSize[i] = getIntegerSizeFromPercentage(sampleSize[i], trainingPercentage);
            sampleCount[i] = 0;
            i++;
        }
    }

    private int getIntegerSizeFromPercentage(int totalSize, int percentage) {
        return Math.round(totalSize * ((float) percentage / 100));
    }

    @Override
    protected void reduce(Text key, Iterable<AvroValue<Record>> values, Context context)
            throws IOException, InterruptedException {
        // Key format: classLabel-randomOrder
        // Ex.0-0.1234567891234567
        for (AvroValue<Record> value : values) {
            outKey.datum(value.datum());
            String classLabel = getClassLabel(key);
            createSampleBySamplingRate(classLabel);
        }
    }

    private String getClassLabel(Text key) {
        String[] result = key.toString().split("-");
        return result[0];
    }

    private void createSampleBySamplingRate(String classLabel) throws IOException, InterruptedException {
        Integer classIndex = classLabelToClassIndex.get(classLabel);
        if (sampleCount[classIndex] < sampleSize[classIndex]) {
            createSampleByTrainingPercentage(classIndex);
        }
    }

    private void createSampleByTrainingPercentage(int classIndex) throws IOException, InterruptedException {
        if (sampleCount[classIndex] < trainingSize[classIndex]) {
            // write training data to subsets sequentially
            int trainingSetIndex = sampleCount[classIndex] % trainingSetCount;
            String trainingSetName = trainingElements.get(trainingSetIndex).getName();
            outputs.write(trainingSetName, outKey, nullWritable);
            outputs.write(SamplingConfiguration.TRAINING_ALL_PREFIX, outKey, nullWritable);
        } else {
            // write testing data
            outputs.write(testingElement.getName(), outKey, nullWritable);
        }
        sampleCount[classIndex]++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }

}
