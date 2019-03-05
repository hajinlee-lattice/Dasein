package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.FileAggregator;
import com.latticeengines.domain.exposed.modeling.Classifier;

public class PythonProfilingMapper extends PythonMapperBase {

    private static final Logger log = LoggerFactory.getLogger(PythonProfilingMapper.class);

    @Override
    public void setupClassifier(Context context, Classifier classifier) throws IOException, InterruptedException {
        List<String> features = getValues(context);

        classifier.setFeatures(features);
        log.info("Profiling on " + features.size() + " features");
    }

    @Override
    public void writeToContext(Context context) throws IOException, InterruptedException {
        Text value = new Text(super.getHdfsOutputDir());
        context.write(new Text(FileAggregator.PROFILE_AVRO), value);
        if (HdfsUtils.fileExists(context.getConfiguration(), value + "/" + FileAggregator.MODEL_PROFILE_AVRO)) {
            context.write(new Text(FileAggregator.MODEL_PROFILE_AVRO), value);
        }
        context.write(new Text(FileAggregator.DIAGNOSTICS_JSON), value);
    }
}
