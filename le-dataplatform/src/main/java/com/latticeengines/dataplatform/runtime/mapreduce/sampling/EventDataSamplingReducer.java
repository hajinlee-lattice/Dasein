package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EventDataSamplingReducer extends
 Reducer<Text, AvroValue<Record>, AvroKey<Record>, NullWritable> {
    private AvroMultipleOutputs outputs = null;
    private static final NullWritable nullWritable = NullWritable.get();

    @Override
    public void setup(Context context) {
        outputs = new AvroMultipleOutputs(context);
    }


    @Override
    protected void reduce(Text key, Iterable<AvroValue<Record>> values,
            Context context) throws IOException, InterruptedException {
        
        for (AvroValue<Record> value : values) {
            outputs.write(key.toString(), new AvroKey<Record>(value.datum()), nullWritable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }

}
