package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;

public abstract class ProfilingAggregator implements FileAggregator {

    @SuppressWarnings("rawtypes")
    @Override
    public void aggregate(List<String> localPaths, Configuration config, Context context) throws Exception {
        aggregateToLocal(localPaths);
        copyToHdfs(config);
    }

    @VisibleForTesting
    abstract void aggregateToLocal(List<String> localPaths) throws Exception;

    protected void copyToHdfs(Configuration config) throws Exception {
        String hdfsPath = config.get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.copyLocalToHdfs(config, getName(), hdfsPath);
    }

    @Override
    public abstract String getName();

}
