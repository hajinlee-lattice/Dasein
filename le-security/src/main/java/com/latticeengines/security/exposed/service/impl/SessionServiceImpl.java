package com.latticeengines.security.exposed.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.service.SessionService;

@Component("sessionService")
public class SessionServiceImpl implements SessionService {

    @SuppressWarnings("unused")
    private static final Log LOGGER = LogFactory.getLog(SessionServiceImpl.class);

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Override
    public Session attach(Ticket ticket){
        Session session = globalSessionManagementService.attach(ticket);
        interpretGARights(session);
        return session;
    }

    @Override
    public Session retrieve(Ticket ticket){
        Session session = globalSessionManagementService.retrieve(ticket);
        interpretGARights(session);
        return session;
    }

    private void interpretGARights(Session session) {
        List<String> GARights = session.getRights();
        try {
            AccessLevel level = AccessLevel.findAccessLevel(GARights);
            session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
            session.setAccessLevel(level.name());
        } catch (Exception e) {
            LOGGER.error("Failed to interpret GA rights: " + GARights.toString(), e);
        }
    }
}
