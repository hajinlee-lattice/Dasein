package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.FileAggregator;
import com.latticeengines.domain.exposed.modeling.Classifier;

public class PythonModelingMapper extends PythonMapperBase {

    private static final Logger log = LoggerFactory.getLogger(PythonModelingMapper.class);

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
