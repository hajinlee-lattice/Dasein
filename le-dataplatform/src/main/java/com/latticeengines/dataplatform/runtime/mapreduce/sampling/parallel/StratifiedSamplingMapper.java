package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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
import com.latticeengines.domain.exposed.modeling.SamplingProperty;

public class StratifiedSamplingMapper extends Mapper<AvroKey<Record>, NullWritable, Text, AvroValue<Record>> {
    private String targetColumnName;
    private Set<String> classLabels;

    private Text outKey;
    private AvroValue<Record> outValue;
    private Random random;

    @Override
    protected void setup(Context context) {
        Configuration config = context.getConfiguration();
        String sampleConfigStr = config.get(EventDataSamplingJob.LEDP_SAMPLE_CONFIG);
        SamplingConfiguration sampleConfig = JsonUtils.deserialize(sampleConfigStr, SamplingConfiguration.class);

        setupSamplingTypeProperty(sampleConfig);

        outKey = new Text();
        outValue = new AvroValue<Record>();
        random = new Random();
    }

    private void setupSamplingTypeProperty(SamplingConfiguration sampleConfig) {
        targetColumnName = sampleConfig.getProperty(SamplingProperty.TARGET_COLUMN_NAME.name());
        String classDistributionString = sampleConfig.getProperty(SamplingProperty.CLASS_DISTRIBUTION.name());

        setupClassLabel(classDistributionString);
    }

    private void setupClassLabel(String classDistributionString) {
        // classDistributionString format: "0=1234,1=4567"
        String[] classLabelAndDistributions = classDistributionString.split(",");
        classLabels = new HashSet<String>(classLabelAndDistributions.length);
        for (String classLabelAndDistribution : classLabelAndDistributions) {
            String[] result = classLabelAndDistribution.split("=");
            classLabels.add(result[0]);
        }
    }

    protected void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,
            InterruptedException {

        Record record = key.datum();
        String classLabel = record.get(targetColumnName).toString();

        if (classLabels.contains(classLabel)) {
            Double randomOrder = random.nextDouble();

            // Concatenate classLabel and a random double for randomizing the
            // data by Hadoop sort stage
            // Format: classLabel-randomOrder
            // Ex.0-0.1234567891234567
            String classLabelAndRandomOrder = classLabel + "-" + String.format("%.16f", randomOrder);

            outKey.set(classLabelAndRandomOrder);
            outValue.datum(key.datum());
            context.write(outKey, outValue);
        }
    }
}
