package com.latticeengines.monitor.exposed.metrics;

import org.aspectj.lang.ProceedingJoinPoint;

public interface MetricsAspect {

    Object logRestApi(ProceedingJoinPoint joinPoint) throws Throwable;

    Object logRestApiCall(ProceedingJoinPoint joinPoint) throws Throwable;

    String getLogRestApiSpecificMetrics();

    String getLogRestApiCallSpecificMetrics();
}
