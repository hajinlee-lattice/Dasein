package com.latticeengines.testframework.service.impl;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

/**
 * Analyzer with a fix retry limit.
 */
public class SimpleRetryAnalyzer implements IRetryAnalyzer {

    private static final int RETRY_COUNT = 3;

    private int count = 0;

    public synchronized boolean canRetry() {
        return count < RETRY_COUNT;
    }

    @Override
    public synchronized boolean retry(ITestResult result) {
        if (count < RETRY_COUNT) {
            count++;
            return true;
        }
        return false;
    }
}
