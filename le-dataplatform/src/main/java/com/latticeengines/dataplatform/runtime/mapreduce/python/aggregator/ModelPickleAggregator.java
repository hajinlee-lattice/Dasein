package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonInvoker;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRUtils;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;

public class ModelPickleAggregator implements FileAggregator {

    @Override
    public void aggregate(List<String> localPaths, Configuration config, Progressable progressable) {
        String metadata = config.get(PythonContainerProperty.METADATA_CONTENTS.name());
        Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
        String script = new AggregationAlgorithm().getScript();
        String version = config.get(MapReduceProperty.VERSION.name());
        script = PythonMRUtils.getScriptPathWithVersion(script, version);
        classifier.setPythonScriptHdfsPath(script);

        ProgressMonitor monitor = null;
        String runtimeConfigFile = null;
        if (progressable != null) {
            monitor = new ProgressMonitor(progressable);
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
