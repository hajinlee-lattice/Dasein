package com.latticeengines.query.evaluator.sparksql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;

public class SparkSQLTestInterceptor implements IMethodInterceptor {

    private static final Logger log = LoggerFactory.getLogger(SparkSQLTestInterceptor.class);

    @Override
    public List<IMethodInstance> intercept(List<IMethodInstance> list, ITestContext iTestContext) {
        List<String> groups = Arrays.asList(iTestContext.getIncludedGroups());
        if (CollectionUtils.isNotEmpty(groups) && !groups.contains("spark")) {
            log.info("Groups [" + StringUtils.join(groups, ", ") + "] does not contain spark. " + //
                    "Skip all tests.");
            return Collections.emptyList();
        } else {
            return list;
        }
    }
}
