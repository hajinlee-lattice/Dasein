package com.latticeengines.pls.metrics.impl;

import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.monitor.exposed.metrics.MetricsAspect;
import com.latticeengines.monitor.exposed.metrics.impl.BaseMetricsAspectImpl;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

public class PlsMetricsAspectImpl extends BaseMetricsAspectImpl implements MetricsAspect {

    @Override
    public String getLogRestApiSpecificMetrics(ProceedingJoinPoint joinPoint) {
        StringBuffer args = new StringBuffer();
        for (Object arg : joinPoint.getArgs()) {
            args.append((arg == null ? " " : arg.toString()) + ";");
        }

        String user = MultiTenantContext.getEmailAddress();
        String tenant = MultiTenantContext.isContextSet() ? MultiTenantContext.getTenant().getId() : "<not set>";

        String metrics = (args.length() == 0) ? ""
                : String.format(" Arguments=%s", args.deleteCharAt(args.length() - 1));

        return metrics + String.format(" User=%s Tenant=%s", user, tenant);
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
