package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;

public interface GlobalAuthTicketDao extends BaseDao<GlobalAuthTicket> {

    int TicketInactivityTimeoutInMinute = 1440;

    List<GlobalAuthTicket> findTicketsByUserIdAndTenant(Long userId, GlobalAuthTenant tenantData);

}
