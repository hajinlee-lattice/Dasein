package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DefaultSamplingReducer extends Reducer<Text, AvroValue<Record>, AvroKey<Record>, NullWritable> {

    private AvroMultipleOutputs outputs;
    private AvroKey<Record> outKey = new AvroKey<Record>();
    private static final NullWritable nullWritable = NullWritable.get();

    @Override
    public void setup(Context context) {
        outputs = new AvroMultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<AvroValue<Record>> values, Context context) throws IOException,
            InterruptedException {
        for (AvroValue<Record> value : values) {
            outKey.datum(value.datum());
            outputs.write(key.toString(), outKey, nullWritable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }
}
