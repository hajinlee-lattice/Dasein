package com.latticeengines.datacloudapi.api.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.datacloud.core.util.HdfsPodContext;

@Aspect
public class ResourceAspect {

    @Before("execution(* com.latticeengines.datacloudapi.api.controller.IngestionResource.*(..))")
    public void allIngestionApis(JoinPoint joinPoint) {
        setHdfsPodContext(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloudapi.api.controller.TransformationResource.*(..))")
    public void allTransformationApis(JoinPoint joinPoint) {
        setHdfsPodContext(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloudapi.api.controller.PublicationResource.*(..))")
    public void allPublicationApis(JoinPoint joinPoint) {
        setHdfsPodContext(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloudapi.api.controller.OrchestrationResource.*(..))")
    public void allOrchestrationApis(JoinPoint joinPoint) {
        setHdfsPodContext(joinPoint);
    }

    private void setHdfsPodContext(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        String hdfsPod = (String) args[args.length - 1];
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        } else {
            HdfsPodContext.changeHdfsPodId(HdfsPodContext.getDefaultHdfsPodId());
        }
    }
}
