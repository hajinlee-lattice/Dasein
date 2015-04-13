package com.latticeengines.scoring.runtime.mapreduce;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EventDataScoringReducer extends Reducer<Text, Text, AvroKey<Record>, NullWritable> {

}
