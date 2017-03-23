package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonInvoker;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRUtils;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;

public class ModelPickleAggregator implements FileAggregator {

    @SuppressWarnings("rawtypes")
    @Override
    public void aggregate(List<String> localPaths, Configuration config, Context context) throws Exception {
        String metadata = config.get(PythonContainerProperty.METADATA_CONTENTS.name());
        Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
        String script = new AggregationAlgorithm().getScript();
        String afterPart = StringUtils.substringAfter(script, "/app");
        String version = config.get(MapReduceProperty.VERSION.name());
        script = "/app/" + version + afterPart;
        classifier.setPythonScriptHdfsPath(script);

        ProgressMonitor monitor = null;
        String runtimeConfigFile = null;
        if (context != null) {
            monitor = new ProgressMonitor(context);
            runtimeConfigFile = PythonMRUtils.getRuntimeConfig(config, monitor);
        }
        PythonInvoker invoker = new PythonInvoker(classifier, runtimeConfigFile);
        if (monitor != null)
            monitor.start();
        try {
            invoker.callLauncher(config);
        } finally {
            if (monitor != null)
                monitor.stop();
        }
    }

    @Override
    public String getName() {
        return FileAggregator.MODEL_PICKLE;
    }
}
