package com.latticeengines.dataplatform.runtime.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EventDataMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    private int sampleSize;
    private static int ALL = -1;

    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        sampleSize = config.getInt("le.sample.size", ALL);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // do reservoir sampling if sample size > 0
        if (sampleSize > 0) {
            int randomValue = (int) Math.random() * 99;
            if (randomValue > sampleSize) {
                return;
            }
        }
    }
}
