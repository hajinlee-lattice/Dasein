package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;

public interface GlobalAuthTicketDao extends BaseDao<GlobalAuthTicket> {

    List<GlobalAuthTicket> findTicketsByUserIdsAndLastAccessDateAndTenant(List<Long> userIds, GlobalAuthTenant tenantData);

    List<GlobalAuthTicket> findTicketsByUserIdAndExternalIssuer(Long userId, String issuer);

    List<GlobalAuthTicket> findByUserAndTenantIdAndNotInTicketAndLastAccessDate(Long tenantId, Long userId, String ticket);
}
