package com.latticeengines.apps.cdl.service;

public interface DataFeedExecutionCleanupService {

    boolean removeStuckExecution(String jobArguments);

}
