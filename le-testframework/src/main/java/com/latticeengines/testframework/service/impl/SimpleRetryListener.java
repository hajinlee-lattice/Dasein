package com.latticeengines.testframework.service.impl;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

/**
 * Listener that retry failed test for a fix amount of times.
 */
public class SimpleRetryListener extends TestListenerAdapter {

    @Override
    public void onTestFailure(ITestResult result) {
        SimpleRetryAnalyzer analyzer = getAnalyzer(result);
        if (analyzer != null) {
            if (analyzer.canRetry()) {
                result.setStatus(ITestResult.SKIP);
            } else {
                // only fail after all retry attempts are exhausted
                result.setStatus(ITestResult.FAILURE);
            }
        }
    }

    private SimpleRetryAnalyzer getAnalyzer(ITestResult result) {
        if (result == null || result.getMethod() == null) {
            return null;
        }

        IRetryAnalyzer analyzer = result.getMethod().getRetryAnalyzer();
        if (!(analyzer instanceof SimpleRetryAnalyzer)) {
            return null;
        } else {
            return (SimpleRetryAnalyzer) analyzer;
        }
    }
}
