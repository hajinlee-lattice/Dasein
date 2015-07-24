package com.latticeengines.pls.metrics.impl;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.monitor.exposed.metrics.MetricsAspect;
import com.latticeengines.monitor.exposed.metrics.impl.BaseMetricsAspectImpl;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

public class PlsMetricsAspectImpl extends BaseMetricsAspectImpl implements MetricsAspect {

    @Override
    public String getLogRestApiCallSpecificMetrics() {
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
