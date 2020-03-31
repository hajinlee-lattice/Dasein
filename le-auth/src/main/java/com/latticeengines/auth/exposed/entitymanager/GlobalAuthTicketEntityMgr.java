package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;

public interface GlobalAuthTicketEntityMgr extends BaseEntityMgr<GlobalAuthTicket> {

    GlobalAuthTicket findByTicket(String ticket);

    List<GlobalAuthTicket> findTicketsByUserIdsAndLastAccessDateAndTenant(List<Long> userIds, GlobalAuthTenant tenantData);

    List<GlobalAuthTicket> findTicketsByUserIdAndExternalIssuer(Long userId, String issuer);

    List<GlobalAuthTicket> findByUserAndTenantIdAndNotInTicketAndLastAccessDate(Long tenantId, Long userId, String ticket);

}
