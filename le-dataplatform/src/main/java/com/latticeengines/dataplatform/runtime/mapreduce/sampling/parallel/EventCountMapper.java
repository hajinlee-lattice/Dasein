package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingProperty;

public class EventCountMapper extends Mapper<AvroKey<Record>, NullWritable, Text, NullWritable> {
    private String counterGroupName;
    private String targetColumnName;

    @Override
    protected void setup(Context context) {
        Configuration config = context.getConfiguration();
        String sampleConfigStr = config.get(EventCounterJob.LEDP_EVENT_COUNTER_CONFIG);
        EventCounterConfiguration sampleConfig = JsonUtils.deserialize(sampleConfigStr,
                EventCounterConfiguration.class);
        targetColumnName = sampleConfig.getProperty(SamplingProperty.TARGET_COLUMN_NAME.name());
        counterGroupName = sampleConfig.getProperty(SamplingProperty.COUNTER_GROUP_NAME.name());
    }

    @Override
    protected void map(AvroKey<Record> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        Record record = key.datum();
        String classLabel = record.get(targetColumnName).toString();
        context.getCounter(counterGroupName, classLabel).increment(1);
    }
}
