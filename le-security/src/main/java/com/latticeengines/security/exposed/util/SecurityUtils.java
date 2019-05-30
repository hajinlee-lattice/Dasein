package com.latticeengines.security.exposed.util;
import javax.servlet.http.HttpServletRequest;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.UserService;
public final class SecurityUtils {
    public static Tenant getTenantFromRequest(HttpServletRequest request,
                                              SessionService sessionService) {
        Session session = getSessionFromRequest(request, sessionService);
        return session.getTenant();
    }
    public static User getUserFromRequest(HttpServletRequest request,
                                          SessionService sessionService,
                                          UserService userService) {
        Session session = getSessionFromRequest(request, sessionService);
        String email = session.getEmailAddress();
        User user = userService.findByEmail(email);
        user.setAccessLevel(session.getAccessLevel());
        return user;
    }
    private static Session getSessionFromRequest(HttpServletRequest request,
                                                 SessionService sessionService) {
        try {
            Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            return sessionService.retrieve(ticket);
        } catch (LedpException e) {
            throw new LoginException(e);
        }
    }
}