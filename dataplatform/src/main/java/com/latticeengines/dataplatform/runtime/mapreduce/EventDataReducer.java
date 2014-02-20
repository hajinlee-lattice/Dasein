package com.latticeengines.dataplatform.runtime.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EventDataReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void setup(Context context) {
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

    }

    public static void main(String[] args) {
    }

}
