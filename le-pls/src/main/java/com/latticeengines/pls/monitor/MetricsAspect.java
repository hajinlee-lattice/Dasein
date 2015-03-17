package com.latticeengines.pls.monitor;

import java.util.UUID;

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

    ThreadLocal<String> tracker = new ThreadLocal<>();

    @Around("execution(* com.latticeengines.pls.controller.*.*(..)) && @annotation(com.wordnik.swagger.annotations.ApiOperation)")
    public Object logRestApi(ProceedingJoinPoint joinPoint) throws Throwable {

        String trackId = tracker.get();
        if (trackId == null) {
            trackId = UUID.randomUUID().toString();
            tracker.set(trackId);
        }
        try {
            return logMetrics(joinPoint, trackId);

        } finally {
            tracker.remove();
        }
    }

    @Around("execution(public * com.latticeengines.pls.globalauth.authentication.impl.*.*(..))")
    public Object logGlobalAuth(ProceedingJoinPoint joinPoint) throws Throwable {
        String trackId = tracker.get();
        return logMetrics(joinPoint, trackId);
    }

    private Object logMetrics(ProceedingJoinPoint joinPoint, String trackId) throws Throwable {

        long startTime = System.currentTimeMillis();

        Object retVal = joinPoint.proceed();

        long endTime = System.currentTimeMillis();

        String ticketId = "";
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            ticketId = token.getSession().getTicket().getUniqueness();
        }
        
        log.info(String.format("Metrics for API=%s ElapsedTime=%d ms Track Id=%s Ticket Id=%s", joinPoint.getSignature()
                .toShortString(), endTime - startTime, trackId, ticketId));

        return retVal;
    }
}
