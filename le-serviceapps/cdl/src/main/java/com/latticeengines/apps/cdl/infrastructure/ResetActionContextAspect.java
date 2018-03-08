package com.latticeengines.apps.cdl.infrastructure;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.apps.cdl.util.ActionContext;

@Aspect
public class ResetActionContextAspect {

    @Before("@annotation(com.latticeengines.apps.cdl.annotation.Action)")
    public void resetActionContextBeforehand(JoinPoint joinPoint) {
        ActionContext.remove();
    }

    @After("@annotation(com.latticeengines.apps.cdl.annotation.Action)")
    public void resetActionContextAfterwards(JoinPoint joinPoint) {
        ActionContext.remove();
    }

}
