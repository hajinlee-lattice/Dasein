package com.latticeengines.security.exposed.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.SessionService;

@Component("sessionService")
public class SessionServiceImpl implements SessionService {
    private static final Log LOGGER = LogFactory.getLog(SessionServiceImpl.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Override
    public Session attach(Ticket ticket){
        return globalSessionManagementService.attach(ticket);
    }

    @Override
    public Session retrieve(Ticket ticket){
        return globalSessionManagementService.retrieve(ticket);
    }
}
