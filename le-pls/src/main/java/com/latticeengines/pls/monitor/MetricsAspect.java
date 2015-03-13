package com.latticeengines.pls.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.pls.security.TicketAuthenticationToken;

@Aspect
public class MetricsAspect {

    static Log log = LogFactory.getLog(MetricsAspect.class);

    @Around("execution(* com.latticeengines.pls.controller.*.*(..)) && @annotation(com.wordnik.swagger.annotations.ApiOperation)")
    public Object logRestApi(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMetrics(joinPoint);
    }

    @Around("execution(public * com.latticeengines.pls.globalauth.authentication.impl.*.*(..))")
    public Object logGlobalAuth(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMetrics(joinPoint);
    }

    private Object logMetrics(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();

        Object retVal = joinPoint.proceed();

        long endTime = System.currentTimeMillis();

        String ticketId = null;
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            ticketId = token.getSession().getTicket().getUniqueness();
        }

        log.info(String.format("Metrics for API=%s ElapsedTime=%d ms Ticket Id=%d", joinPoint.getSignature()
                .toShortString(), endTime - startTime, ticketId));

        return retVal;
    }
}
