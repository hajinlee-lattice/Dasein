package com.latticeengines.dataplatform.runtime.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class EventDataProcessorJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf();

		Job job = new Job(jobConf, "muxdemux_job");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setJarByClass(EventDataProcessorJob.class);

		//AvroJob.setInputSchema(jobConf, IN_SCHEMA);
		//AvroJob.setOutputSchema(jobConf, OUT_SCHEMA);

		job.setMapperClass(EventDataMapper.class);
		job.setReducerClass(EventDataReducer.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
