package com.latticeengines.testframework.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import com.latticeengines.db.exposed.util.DBConnectionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;

public class ContextResetTestListener implements ITestListener {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ContextResetTestListener.class);

    @Override
    public void onTestStart(ITestResult result) {
    }

    @Override
    public void onTestSuccess(ITestResult result) {
    }

    @Override
    public void onTestFailure(ITestResult result) {
    }

    @Override
    public void onTestSkipped(ITestResult result) {
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    }

    @Override
    public void onStart(ITestContext context) {
    }

    @Override
    public void onFinish(ITestContext context) {
        DBConnectionContext.setReaderConnection(false);
        MultiTenantContext.clearTenant();
    }

}
