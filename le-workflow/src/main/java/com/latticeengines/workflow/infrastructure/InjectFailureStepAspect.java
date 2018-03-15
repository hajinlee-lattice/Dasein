package com.latticeengines.workflow.infrastructure;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class InjectFailureStepAspect {

    private static final Logger log = LoggerFactory.getLogger(InjectFailureStepAspect.class);

    @Pointcut("this(com.latticeengines.workflow.exposed.build.BaseWorkflowStep)")
    public void workflowStep() {
    }

    @Before("this(com.latticeengines.workflow.exposed.build.BaseWorkflowStep)")
    public void baseWorkflowStep(JoinPoint joinPoint) {
        Object targetStep = joinPoint.getTarget();
        log.info("targetStep=" + targetStep.getClass().getSimpleName());
    }

}
