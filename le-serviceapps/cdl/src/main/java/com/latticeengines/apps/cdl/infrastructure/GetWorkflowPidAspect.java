package com.latticeengines.apps.cdl.infrastructure;

import javax.inject.Inject;

import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Aspect
public class GetWorkflowPidAspect {
    private static final Logger log = LoggerFactory.getLogger(GetWorkflowPidAspect.class);

    @Inject
    private WorkflowProxy workflowProxy;

    @Around("@annotation(com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid)")
    public Object submitMethodsOfWorkflowSubmitters(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs();
        if (args[0] != null) {
            String customerSpace = String.valueOf(args[0]);
            Long pid = workflowProxy.createWorkflowJob(customerSpace);
            ((WorkflowPidWrapper) args[args.length - 1]).setPid(pid);
            return joinPoint.proceed(joinPoint.getArgs());
        } else {
            throw new RuntimeException("Customerspace not passed in when create workflowjob.");
        }
    }
}
