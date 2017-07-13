package com.latticeengines.testframework.security.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import com.latticeengines.testframework.security.GlobalAuthTestBed;

public class GlobalAuthCleanupTestListener implements ITestListener {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthCleanupTestListener.class);

    private AtomicBoolean testFailed = new AtomicBoolean(false);

    @Override
    public void onTestStart(ITestResult result) {
    }

    @Override
    public void onTestSuccess(ITestResult result) {
    }

    @Override
    public void onTestFailure(ITestResult result) {
        log.info("Executing onTestFailure() in GlobalAuthCleanupTestListener.");
        testFailed.set(true);
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
        log.info("Executing onFinish() in GlobalAuthCleanupTestListener.");
        GlobalAuthTestBed testBed = getTestBedFromContext(context);

        if (testBed != null) {
            log.info("Cleanup DL and ZK.");
            testBed.cleanupDlZk();

            if (testFailed.get()) {
                log.info("There are failed test, so skip cleaning up PLS and HDFS.");
            } else {
                log.info("Cleanup PLS and HDFS.");
                testBed.cleanupPlsHdfs();
            }
        } else {
            log.warn("Did not find any GlobalAuthTestBed instance in test context.");
        }
    }

    private GlobalAuthTestBed getTestBedFromContext(ITestContext context) {
        Set<ITestResult> resultSet = new HashSet<>(context.getPassedTests().getAllResults());
        resultSet.addAll(context.getFailedTests().getAllResults());

        if (resultSet.isEmpty()) {
            log.warn("No test results found in TestNG context.");
        }

        for (ITestResult result: resultSet) {
            log.info("Search for field of type GlobalAuthTestBed");
            Class<?> clazz = result.getTestClass().getRealClass();
            List<Field> fields = new ArrayList<>();
            fields = getAllFields(fields, clazz);
            for (Field field: fields) {
                Class<?> fieldClz = field.getType();
                if (GlobalAuthTestBed.class.isAssignableFrom(fieldClz)) {
                    log.info("Found the test bed instance: " + field.getName());
                    field.setAccessible(true);
                    try {
                        return (GlobalAuthTestBed) field.get(result.getInstance());
                    } catch (IllegalAccessException e) {
                        log.error(e.getMessage(), e);
                        return null;
                    }
                }
            }
        }

        log.warn("Cannot find any field of type GlobalAuthTestBed");
        return null;
    }

    public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        if (type != null) {
            try {
                if (type.getDeclaredFields() != null && type.getDeclaredFields().length > 0) {
                    fields.addAll(Arrays.asList(type.getDeclaredFields()));
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }

        return fields;
    }

}
