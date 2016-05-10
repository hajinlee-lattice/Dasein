package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.FileAggregator;
import com.latticeengines.domain.exposed.modeling.Classifier;

public class PythonModelingMapper extends PythonMapperBase {

    private static final Log log = LogFactory.getLog(PythonModelingMapper.class);

    @Override
    public void setupClassifier(Context context, Classifier classifier) throws IOException, InterruptedException {
        List<String> trainingSetList = getValues(context);
        String trainingSet = trainingSetList.get(0);

        String trainingPath = StringUtils.substringBeforeLast(classifier.getTrainingDataHdfsPath(), "/") + "/"
                + trainingSet;
        classifier.setTrainingDataHdfsPath(trainingPath);
        log.info("Training on " + trainingSet);
    }

    @Override
    public void writeToContext(Context context) throws IOException, InterruptedException {
        Text value = new Text(super.getHdfsOutputDir());
        context.write(new Text(FileAggregator.MODEL_PICKLE), value);
        context.write(new Text(FileAggregator.FEATURE_IMPORTANCE_TXT), value);
    }
}
