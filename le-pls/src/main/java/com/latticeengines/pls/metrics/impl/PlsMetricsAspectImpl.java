package com.latticeengines.pls.metrics.impl;

import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.monitor.exposed.metrics.MetricsAspect;
import com.latticeengines.monitor.exposed.metrics.impl.BaseMetricsAspectImpl;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class PlsMetricsAspectImpl extends BaseMetricsAspectImpl implements MetricsAspect {

    @Override
    public String getLogRestApiSpecificMetrics(ProceedingJoinPoint joinPoint) {
        StringBuffer args = new StringBuffer();
        for (Object arg : joinPoint.getArgs()) {
            args.append((arg == null ? " " : arg.toString()) + ";");
        }
        if (args.length() == 0) {
            return "";
        }
        String metrics = String.format(" Arguments=%s", args.deleteCharAt(args.length() - 1));

        String user = MultiTenantContext.getEmailAddress();
        return metrics + String.format(" User=%s", user);
    }

    @Override
    public String getLogRestApiCallSpecificMetrics(ProceedingJoinPoint joinPoint) {
        String ticketId = "";
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            if (token.getSession() != null && token.getSession().getTicket() != null
                    && token.getSession().getTicket().getUniqueness() != null) {
                ticketId = token.getSession().getTicket().getUniqueness();
            }
        }
        return String.format(" Ticket Id=%s", ticketId);
    }
}
