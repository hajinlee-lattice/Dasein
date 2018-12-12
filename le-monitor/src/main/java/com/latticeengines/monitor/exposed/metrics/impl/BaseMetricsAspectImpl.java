package com.latticeengines.monitor.exposed.metrics.impl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import com.latticeengines.monitor.exposed.metrics.MetricsAspect;

@Aspect
public class BaseMetricsAspectImpl implements MetricsAspect {

    public static Logger log = LoggerFactory.getLogger(BaseMetricsAspectImpl.class);

    ThreadLocal<String> tracker = new ThreadLocal<>();

    @Around("@annotation(io.swagger.annotations.ApiOperation) && !@annotation(com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog)")
    public Object logRestApi(ProceedingJoinPoint joinPoint) throws Throwable {

        String trackId = tracker.get();
        if (trackId == null) {
            trackId = UUID.randomUUID().toString();
            tracker.set(trackId);
        }
        try {
            return logRestApiMetrics(joinPoint, trackId);

        } finally {
            tracker.remove();
        }
    }

    @Around("@annotation(com.latticeengines.domain.exposed.monitor.annotation.RestApiCall)")
    public Object logRestApiCall(ProceedingJoinPoint joinPoint) throws Throwable {

        String trackId = tracker.get();
        if (trackId == null) {
            trackId = UUID.randomUUID().toString();
            tracker.set(trackId);
        }
        try {
            return logRestApiCallMetrics(joinPoint, trackId);

        } finally {
            tracker.remove();
        }
    }

    public Object logRestApiMetrics(ProceedingJoinPoint joinPoint, String trackId) throws Throwable {
        long startTime = System.currentTimeMillis();

        Object retVal = joinPoint.proceed();

        long endTime = System.currentTimeMillis();

        long elapsedTime = endTime - startTime;

        String metricsToLog = getDefaultMetrics(joinPoint.getSignature(), elapsedTime, trackId)
                + getLogRestApiSpecificMetrics(joinPoint);
        log.debug(metricsToLog);

        return retVal;
    }

    public Object logRestApiCallMetrics(ProceedingJoinPoint joinPoint, String trackId) throws Throwable {

        long startTime = System.currentTimeMillis();

        Object retVal = joinPoint.proceed();

        long endTime = System.currentTimeMillis();

        long elapsedTime = endTime - startTime;

        String metricsToLog = getDefaultMetrics(joinPoint.getSignature(), elapsedTime, trackId)
                + getLogRestApiCallSpecificMetrics(joinPoint);
        log.debug(metricsToLog);

        return retVal;
    }

    @Override
    public String getLogRestApiSpecificMetrics(ProceedingJoinPoint joinPoint) {
        return "";
    }

    private String getDefaultMetrics(Signature apiSignature, long elapsedTime, String trackId) {
        return String.format("Metrics for API=%s ElapsedTime=%d ms Track Id=%s", apiSignature.toShortString(),
                elapsedTime, trackId);
    }

    @Override
    public String getLogRestApiCallSpecificMetrics(ProceedingJoinPoint joinPoint) {
        return "";
    }

}
