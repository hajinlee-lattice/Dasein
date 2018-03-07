package com.latticeengines.apps.cdl.controller;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.apps.cdl.util.ActionContext;

@Aspect
public class ResetActionContextAspect {

    @Before("execution(* com.latticeengines.apps.cdl.controller.*AndActionDTO(..))")
    public void resetActionContextBeforehand(JoinPoint joinPoint) {
        ActionContext.remove();
    }

    @After("execution(* com.latticeengines.apps.cdl.controller.*AndActionDTO(..))")
    public void resetActionContextAfterwards(JoinPoint joinPoint) {
        ActionContext.remove();
    }

}
