package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonInvoker;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;

public class ModelPickleAggregator implements FileAggregator {

    @Override
    public void aggregate(List<String> localPaths, Configuration config) throws Exception {
        String metadata = config.get(PythonContainerProperty.METADATA_CONTENTS.name());
        Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
        classifier.setPythonScriptHdfsPath(new AggregationAlgorithm().getScript());

        PythonInvoker invoker = new PythonInvoker(classifier);
        invoker.callLauncher(config);
    }

    @Override
    public String getName() {
        return FileAggregator.MODEL_PICKLE;
    }
}
