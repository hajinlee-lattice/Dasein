package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.FileAggregator;
import com.latticeengines.domain.exposed.modeling.Classifier;

public class PythonProfilingMapper extends PythonMapperBase {

    private static final Log log = LogFactory.getLog(PythonProfilingMapper.class);

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
        context.write(new Text(FileAggregator.DIAGNOSTICS_JSON), value);
    }
}
