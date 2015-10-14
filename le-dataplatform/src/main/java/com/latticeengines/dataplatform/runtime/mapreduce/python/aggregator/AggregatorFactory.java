package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.util.HashMap;
import java.util.Map;

public class AggregatorFactory {

    private Map<String, FileAggregator> fileAggregators;
    private static AggregatorFactory factory;

    private AggregatorFactory() {
        fileAggregators = new HashMap<String, FileAggregator>();
    }

    public static AggregatorFactory getInstance() {
        if (factory == null) {
            synchronized (AggregatorFactory.class) {
                if (factory == null) {
                    factory = new AggregatorFactory();
                }
            }
        }
        return factory;
    }

    public void registerFileAggregators(Map<String, FileAggregator> fileAggregators) {
        this.fileAggregators = fileAggregators;
    }

    public void registerFileAggregator(String type, FileAggregator fileAggregator) {
        fileAggregators.put(type, fileAggregator);
    }

    public FileAggregator getAggregator(String fileType) {
        return fileAggregators.get(fileType);
    }

}
