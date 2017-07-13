package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;

public abstract class PythonMapperBase extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(PythonMapperBase.class);
    private Configuration config;
    private Classifier classifier;
    private String hdfsOutputDir;
    private PythonInvoker invoker;
    private ProgressMonitor monitor;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        config = context.getConfiguration();
        String metadata = config.get(PythonContainerProperty.METADATA_CONTENTS.name());
        classifier = JsonUtils.deserialize(metadata, Classifier.class);
        hdfsOutputDir = addUniqueOutputDir();

        setupClassifier(context, classifier);
        monitor = new ProgressMonitor(context);
        String runtimeConfigFile = PythonMRUtils.getRuntimeConfig(config, monitor);
        invoker = new PythonInvoker(classifier, runtimeConfigFile);
        monitor.start();
    }

    public abstract void setupClassifier(Context context, Classifier classifier)
            throws IOException, InterruptedException;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        log.info("Setting up environment to launch Python process");
        setup(context);
        try {
            invoker.callLauncher(config);
            writeToContext(context);
            log.info("Writing to context after Python process completed");
        } finally {
            monitor.stop();
        }

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
