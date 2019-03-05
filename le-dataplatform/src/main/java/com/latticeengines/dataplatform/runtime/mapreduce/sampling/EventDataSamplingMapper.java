package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public class EventDataSamplingMapper extends Mapper<AvroKey<Record>, NullWritable, Text, AvroValue<Record>> {

    private static final Logger log = LoggerFactory.getLogger(EventDataSamplingMapper.class);

    private SamplingConfiguration sampleConfig = null;
    private Random random = null;

    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        String sampleConfigStr = config.get(EventDataSamplingJob.LEDP_SAMPLE_CONFIG);
        if (sampleConfigStr == null) {
            throw new LedpException(LedpCode.LEDP_12004);
        }
        sampleConfig = JsonUtils.deserialize(sampleConfigStr, SamplingConfiguration.class);
        random = new Random();
        if (sampleConfig.getRandomSeed() != -1) {
            Long seed = sampleConfig.getRandomSeed();
            log.info("Random seed = " + seed);
            random.setSeed(seed);
        } else {
            log.info("No random seed.");
        }

    }

    @Override
    public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,
            InterruptedException {
        List<SamplingElement> samplingElements = sampleConfig.getSamplingElements();
        int trainingPct = sampleConfig.getTrainingPercentage();

        for (SamplingElement samplingElement : samplingElements) {
            int sampleRate = samplingElement.getPercentage();
            int sample = random.nextInt(100);

            if (sample < sampleRate) {
                // now sample for training-test
                int trainingOrTest = random.nextInt(100);
                String name = samplingElement.getName();
                if (trainingOrTest < trainingPct) {
                    name += "Training";
                } else {
                    name += "Test";
                }
                context.write(new Text(name), new AvroValue<Record>(key.datum()));
            }

        }

    }

}
