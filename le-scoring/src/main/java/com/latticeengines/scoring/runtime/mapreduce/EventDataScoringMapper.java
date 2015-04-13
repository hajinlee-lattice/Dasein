package com.latticeengines.scoring.runtime.mapreduce;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, Text, Text>{

}
