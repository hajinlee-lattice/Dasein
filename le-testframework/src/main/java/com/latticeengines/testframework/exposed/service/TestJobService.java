package com.latticeengines.testframework.exposed.service;

import java.util.concurrent.TimeoutException;

public interface TestJobService {
    void waitForProcessAnalyzeAllActionsDone(int maxWaitInMinutes) throws TimeoutException;
}
