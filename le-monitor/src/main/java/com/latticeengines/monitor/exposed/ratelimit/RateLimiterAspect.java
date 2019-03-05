package com.latticeengines.monitor.exposed.ratelimit;

import java.lang.reflect.Method;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

@Aspect
public class RateLimiterAspect implements ApplicationContextAware {

    public static Logger log = LoggerFactory.getLogger(RateLimiterAspect.class);

    private ApplicationContext applicationContext;

    @Around("@annotation(com.latticeengines.monitor.exposed.ratelimit.RateLimit)")
    public Object checkRateLimit(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        RateLimit rateLimit = method.getAnnotation(RateLimit.class);

        if (rateLimit == null) {
            return joinPoint.proceed();
        } else {
            RateCounter rateCounter = applicationContext.getBean(rateLimit.argumentParser());

            if (rateCounter == null) {
                return joinPoint.proceed();
            }
            return checkRateLimit(joinPoint, rateLimit, rateCounter);
        }
    }

    private Object checkRateLimit(ProceedingJoinPoint joinPoint, //
            RateLimit rateLimit, //
            RateCounter rateCounter//
    ) throws Throwable {
        Object[] args = joinPoint.getArgs();
        HttpServletRequest request = rateCounter.getHttpRequest(args);

        int recordCount = 0;

        if (request != null) {
            recordCount = rateCounter.getRecordCount(args);

            if (recordCount > 0) {
                rateCounter.shouldAccept(request, recordCount);
            }
        }

        try {
            return joinPoint.proceed();
        } finally {
            if (recordCount > 0) {
                rateCounter.decrementCounter(recordCount);
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
