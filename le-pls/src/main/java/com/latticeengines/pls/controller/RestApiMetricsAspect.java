package com.latticeengines.pls.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class RestApiMetricsAspect {

    static Log log = LogFactory.getLog(RestApiMetricsAspect.class);

    @Around("execution(* com.latticeengines.pls.controller.*.*(..)) && @annotation(com.wordnik.swagger.annotations.ApiOperation)")
    public Object logMetrics(ProceedingJoinPoint joinPoint) throws Throwable {

        long startTime = System.currentTimeMillis();

        Object retVal = joinPoint.proceed();

        long endTime = System.currentTimeMillis();

        log.info("Metrics for API=" + joinPoint.getSignature().getName() + " ElapsedTime=" + (endTime - startTime)
                + " in milliseconds");

        return retVal;

    }

}
