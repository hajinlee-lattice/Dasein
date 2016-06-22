package com.latticeengines.security.exposed.globalauth.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.entitymanager.GlobalAuthAuthenticationEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.util.GlobalAuthPasswordUtils;

@Component("globalAuthenticationService")
public class GlobalAuthenticationServiceImpl extends GlobalAuthenticationServiceBaseImpl implements
        GlobalAuthenticationService {

    private static final Log log = LogFactory.getLog(GlobalAuthenticationServiceImpl.class);

    @Autowired
    private GlobalAuthAuthenticationEntityMgr gaAuthenticationEntityMgr;

    @Autowired
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Autowired
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Override
    public synchronized Ticket authenticateUser(String user, String password) {
        try {
            log.info(String.format("Authenticating user %s against Global Auth.", user));
            return globalAuthAuthenticateUser(user, password);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18001, e, new String[] { user });
        }

    }

    private Ticket globalAuthAuthenticateUser(String username, String password) throws Exception {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(username);
        if (latticeAuthenticationData == null) {
            throw new Exception("The specified user doesn't exists");
        }

        boolean isActive = latticeAuthenticationData.getGlobalAuthUser().getIsActive();
        if (!isActive) {
            throw new Exception("The user is inactive!");
        }
        Ticket ticket = authenticate(username, password);
        if (ticket != null) {
            return ticket;
        }

        throw new Exception("The credentials provided for login are incorrect.");
    }

    private Ticket authenticate(String username, String password) {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(username);
        if (latticeAuthenticationData == null) {
            return null;
        }
        if (!latticeAuthenticationData.getPassword().equals(GlobalAuthPasswordUtils
                .EncryptPassword(password))) {
            return null;
        }
        Date lastModifiedInUTC = latticeAuthenticationData.getLastModificationDate();
        Ticket ticket = new Ticket();
        ticket.setUniqueness(UUID.randomUUID().toString());
        ticket.setRandomness(GlobalAuthPasswordUtils.GetSecureRandomString(16));
        ticket.setMustChangePassword(latticeAuthenticationData.getMustChangePassword());
        ticket.setPasswordLastModified(lastModifiedInUTC.getTime());
        GlobalAuthTicket ticketData = new GlobalAuthTicket();
        ticketData.setUserId(latticeAuthenticationData.getGlobalAuthUser().getPid());
        ticketData.setTicket(ticket.getData());
        ticketData.setLastAccessDate(new Date(System.currentTimeMillis()));
        GlobalAuthUser userData = gaUserEntityMgr.findByUserIdWithTenantRightsAndAuthentications(ticketData.getUserId());
        if (userData.getUserTenantRights() != null && userData.getUserTenantRights().size() > 0) {
            Map<String, GlobalAuthTenant> distinctTenants = new HashMap<String, GlobalAuthTenant>();
            for (GlobalAuthUserTenantRight rightData : userData.getUserTenantRights()) {
                if (rightData.getGlobalAuthTenant() != null) {
                    if (!distinctTenants.containsKey(rightData.getGlobalAuthTenant().getId())) {
                        distinctTenants.put(rightData.getGlobalAuthTenant().getId(),
                                rightData.getGlobalAuthTenant());
                    }
                }
            }
            List<Tenant> tenants = new ArrayList<Tenant>();
            for (Entry<String, GlobalAuthTenant> tenantData : distinctTenants.entrySet()) {
                Tenant tenant = new Tenant();
                tenant.setId(tenantData.getKey());
                tenant.setName(tenantData.getValue().getName());
                tenants.add(tenant);
            }
            ticket.setTenants(tenants);
        }
        gaTicketEntityMgr.create(ticketData);
        return ticket;
    }

    @Override
    public synchronized boolean discard(Ticket ticket) {
        try {
            log.info("Discarding ticket " + ticket + " against Global Auth.");

            return globalDiscard(ticket);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18009, e, new String[] { ticket.toString() });
        }

    }

    private boolean globalDiscard(Ticket ticket) {
        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        if (ticketData != null) {
            gaTicketEntityMgr.delete(ticketData);
        } else {
            return false;
        }
        return true;
    }

}
