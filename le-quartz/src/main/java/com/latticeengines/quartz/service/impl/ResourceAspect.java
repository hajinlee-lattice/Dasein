package com.latticeengines.quartz.service.impl;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.security.exposed.InternalResourceBase;

@Aspect
public class ResourceAspect {
    private InternalResourceBase internalResourceBase = new InternalResourceBase();

    @Before("execution(* com.latticeengines.quartz.controller.SchedulerResource.*(..))")
    public void allMethods(JoinPoint joinPoint) {
        checkHeader(joinPoint);
    }

    private void checkHeader(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();

        for (Object arg : args) {
            if (arg instanceof HttpServletRequest) {
                internalResourceBase.checkHeader((HttpServletRequest) arg);
            }
        }
    }
}
