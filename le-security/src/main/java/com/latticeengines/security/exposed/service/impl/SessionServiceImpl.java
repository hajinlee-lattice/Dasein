package com.latticeengines.security.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
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
        List<GrantedRight> rights = GrantedRight.getGrantedRights(GARights);

        AccessLevel level = AccessLevel.findAccessLevel(GARights);
        if (level == null) {
            level = upgradeFromDeprecatedBARD(session);
        }

        if (level != null) {
            session.setAccessLevel(level.name());
            rights = removePLSRights(rights);
            rights.addAll(level.getGrantedRights());
        }

        session.setRights(GrantedRight.getAuthorities(rights));
    }

    private List<GrantedRight> removePLSRights(List<GrantedRight> rights) {
        List<GrantedRight> result = new ArrayList<>();
        for (GrantedRight right: rights) {
            if (!right.getAuthority().contains("_PLS_")) {
                result.add(right);
            }
        }
        return result;
    }

    public AccessLevel upgradeFromDeprecatedBARD(Session session) {
        Tenant tenant = session.getTenant();
        if (tenant == null) { return null; }
        String email = session.getEmailAddress();
        if (email == null) { return null; }
        User user = globalUserManagementService.getUserByEmail(email);
        if (user == null) { return null; }
        List<String> rights = session.getRights();

        return upgradeFromDeprecatedBARD(tenant.getId(), user.getUsername(), email, rights);
    }

    @Override
    public AccessLevel upgradeFromDeprecatedBARD(String tenantId, String username, String email, List<String> rights) {

        if (!hasPLSRights(rights)) { return null; }

        AccessLevel level;
        if (isDeprecatedAdmin(rights)) {
            if (isInternalEmail(email)) {
                level = AccessLevel.INTERNAL_ADMIN;
            } else {
                level = AccessLevel.EXTERNAL_ADMIN;
            }
        } else {
            if (isInternalEmail(email)) {
                level = AccessLevel.INTERNAL_USER;
            } else {
                level = AccessLevel.EXTERNAL_USER;
            }
        }
        globalUserManagementService.grantRight(level.name(), tenantId, username);
        LOGGER.info(String.format(
                "User %s (%s) has been upgraded to the role of %s in tenant %s",
                username, email, level.name(), tenantId));

        return level;
    }

    private boolean isDeprecatedAdmin(List<String> rights) {
        return rights.contains(GrantedRight.EDIT_PLS_MODELS.getAuthority()) ||
                rights.contains(GrantedRight.EDIT_PLS_CONFIGURATION.getAuthority());
    }

    private boolean isInternalEmail(String email) {
        return email.toLowerCase().endsWith("lattice-engines.com");
    }

    private boolean hasPLSRights(List<String> rights) {
        for (String right : rights) {
            if (right.contains("_PLS_")) { return true; }
        }
        return false;
    }
}
