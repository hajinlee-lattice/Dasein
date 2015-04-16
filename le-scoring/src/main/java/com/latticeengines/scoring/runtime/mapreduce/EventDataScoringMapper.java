package com.latticeengines.scoring.runtime.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        @SuppressWarnings("deprecation")
        Path[] paths = context.getLocalCacheFiles();
        for (Path p : paths) {
            log.info("files" + p);
        }
        while (context.nextKeyValue()) {
            log.info("key: " + context.getCurrentKey().datum());
        }
        log.info("outputDir: " + context.getConfiguration().get(MapReduceProperty.OUTPUT.name()));
    }
}
