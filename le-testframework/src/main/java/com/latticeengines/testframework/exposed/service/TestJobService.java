package com.latticeengines.testframework.exposed.service;

import java.util.concurrent.TimeoutException;

public interface TestJobService {
    void waitForProcessAnalyzeReady(int maxWaitInMinutes) throws TimeoutException;
}
