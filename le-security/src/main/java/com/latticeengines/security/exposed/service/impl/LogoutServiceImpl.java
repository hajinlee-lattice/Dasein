package com.latticeengines.security.exposed.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.auth.GlobalAuthExternalSession;
import com.latticeengines.domain.exposed.auth.OneLoginExternalSession;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.service.LogoutService;
import com.latticeengines.security.exposed.service.OneLoginService;
import com.latticeengines.security.exposed.service.SessionService;

@Service("logoutService")
public class LogoutServiceImpl implements LogoutService {

    private static final Logger log = LoggerFactory.getLogger(LogoutServiceImpl.class);

    @Inject
    private SessionService sessionService;

    @Inject
    private OneLoginService oneLoginService;

    @Override
    public String logout(String token, String redirectTo) {
        String redirectUrl = null;
        if (StringUtils.isNotBlank(token)) {
            Ticket ticket = new Ticket(token);
            GlobalAuthExternalSession externalSession = sessionService.retrieveExternalSession(ticket);
            sessionService.logout(ticket);
            if (externalSession != null) {
                redirectUrl = logoutExternalSession(redirectTo, externalSession);
            } else {
                log.info("Token " + token + " does not have an external session.");
            }
        }
        if (StringUtils.isNotBlank(redirectUrl)) {
            log.info("Redirect logout request to " + redirectUrl);
        }
        return redirectUrl;
    }

    private String logoutExternalSession(String redirectTo, GlobalAuthExternalSession externalSession) {
        if (OneLoginExternalSession.TYPE.equals(externalSession.getType())) {
            return oneLoginService.logout((OneLoginExternalSession) externalSession, redirectTo);
        } else {
            log.warn("Does not know how to logout external session of type " + externalSession.getType());
            return null;
        }
    }


}
