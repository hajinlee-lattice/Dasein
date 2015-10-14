package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class DefaultSamplingPartitioner extends Partitioner<Text, AvroValue<Record>> {

    @Override
    public int getPartition(Text key, AvroValue<Record> value, int numPartitions) {
        if (key.toString().equals(SamplingConfiguration.TRAINING_ALL_PREFIX))
            return 0;
        else
            return 1;
    }

}
