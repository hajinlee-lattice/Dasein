package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonInvoker;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;

public class ModelPickleAggregator implements FileAggregator {

    @Override
    public void aggregate(List<String> localPaths, Configuration config) throws Exception {
        String metadata = config.get(PythonContainerProperty.METADATA_CONTENTS.name());
        Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
        String script = new AggregationAlgorithm().getScript();
        String afterPart = StringUtils.substringAfter(script, "/app");
        String version = config.get(MapReduceProperty.VERSION.name());
        script = "/app/" + version + afterPart;
        classifier.setPythonScriptHdfsPath(script);

        PythonInvoker invoker = new PythonInvoker(classifier);
        invoker.callLauncher(config);
    }

    @Override
    public String getName() {
        return FileAggregator.MODEL_PICKLE;
    }
}
