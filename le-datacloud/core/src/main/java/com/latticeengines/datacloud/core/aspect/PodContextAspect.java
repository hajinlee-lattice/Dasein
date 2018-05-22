package com.latticeengines.datacloud.core.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;

import com.latticeengines.datacloud.core.util.HdfsPodContext;

@Aspect
public class PodContextAspect {

    @After("@annotation(com.latticeengines.datacloud.core.annotation.PodContextAware)")
    public void afterPodContextAwareController(JoinPoint joinPoint) {
        HdfsPodContext.changeHdfsPodId(null);
    }

}
