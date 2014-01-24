package com.latticeengines.dataplatform.runtime.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EventDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private int samplingRate;
	
	@Override
	public void setup(Context context) {
		Configuration config = context.getConfiguration();
		samplingRate = config.getInt("le.sampling.rate", -1);
	}
	
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// implement sampling
    	if (samplingRate > 0) {
    		int randomValue = (int) Math.random() * 99;
    		if (randomValue > samplingRate) {
    			return;
    		}
    	}
    }
}
