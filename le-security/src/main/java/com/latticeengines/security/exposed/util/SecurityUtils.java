package com.latticeengines.security.exposed.util;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.SessionService;

public final class SecurityUtils {

    public static final Tenant getTenantFromRequest(HttpServletRequest request,
                                                    SessionService sessionService) {
        Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
        return sessionService.retrieve(ticket).getTenant();
    }
}
