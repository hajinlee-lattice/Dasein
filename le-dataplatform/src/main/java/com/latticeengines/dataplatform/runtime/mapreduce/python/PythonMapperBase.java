package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;

public abstract class PythonMapperBase extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log log = LogFactory.getLog(PythonMapperBase.class);
    private Configuration config;
    private Classifier classifier;
    private String hdfsOutputDir;
    private PythonInvoker invoker;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        config = context.getConfiguration();
        String metadata = config.get(PythonContainerProperty.METADATA_CONTENTS.name());
        classifier = JsonUtils.deserialize(metadata, Classifier.class);
        hdfsOutputDir = addUniqueOutputDir();

        setupClassifier(context, classifier);
        invoker = new PythonInvoker(classifier);
    }

    public abstract void setupClassifier(Context context, Classifier classifier) throws IOException,
            InterruptedException;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        log.info("Setting up environment to launch Python process");
        setup(context);

        invoker.callLauncher(config);

        log.info("Writing to context after Python process completed");
        writeToContext(context);
    }

    public abstract void writeToContext(Context context) throws IOException, InterruptedException;

    public List<String> getValues(Context context) throws IOException, InterruptedException {
        List<String> values = new ArrayList<String>();
        while (context.nextKeyValue()) {
            values.add(context.getCurrentValue().toString());
        }
        return values;
    }

    public Classifier getclassifier() {
        return classifier;
    }

    public String getHdfsOutputDir() {
        return hdfsOutputDir;
    }

    private String addUniqueOutputDir() throws IOException, InterruptedException {
        String hdfsPath = config.get(MapReduceProperty.INPUT.name());
        hdfsPath += "/" + config.get(MapReduceProperty.JOB_TYPE.name()) + "_" + UUID.randomUUID().toString();
        classifier.setModelHdfsDir(hdfsPath);
        return hdfsPath;
    }

}
