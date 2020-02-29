package com.latticeengines.security.util;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.service.TenantService;

public final class LoginUtils {

    protected LoginUtils() {
        throw new UnsupportedOperationException();
    }

    public static LoginDocument generateLoginDoc(Ticket ticket, GlobalAuthUserEntityMgr gaUserEntityMgr,
            GlobalAuthTicketEntityMgr gaTicketEntityMgr, TenantService tenantService) {
        LoginDocument doc = new LoginDocument();
        if (ticket == null) {
            doc.setErrors(Collections.singletonList("The email address or password is not valid. Please re-enter your credentials."));
            return doc;
        }
        doc.setRandomness(ticket.getRandomness());
        doc.setUniqueness(ticket.getUniqueness());

        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        GlobalAuthUser userData = gaUserEntityMgr.findByUserId(ticketData.getUserId());
        if (userData == null) {
            doc.setErrors(Collections.singletonList("The email address or password is not valid. Please re-enter your credentials."));
            return doc;
        }
        doc.setUserName(userData.getEmail());
        doc.setFirstName(userData.getFirstName());
        doc.setLastName(userData.getLastName());
        doc.setSuccess(true);

        LoginDocument.LoginResult result = doc.new LoginResult();
        result.setMustChangePassword(ticket.isMustChangePassword());
        result.setPasswordLastModified(ticket.getPasswordLastModified());
        List<Tenant> tenants = tenantService.getTenantsByStatus(TenantStatus.ACTIVE);
        List<Tenant> gaTenants = ticket.getTenants();
        if (CollectionUtils.isNotEmpty(gaTenants)) {
            tenants.retainAll(gaTenants);
            tenants.sort(new TenantNameSorter());
            result.setTenants(tenants);
        }
        doc.setResult(result);

        return doc;
    }

}
