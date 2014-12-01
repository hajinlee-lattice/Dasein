package com.latticeengines.perf.exposed.metric.sink;

public interface SinkOperations {

    String canWrite();

    void sendMetric(String metric);

}
