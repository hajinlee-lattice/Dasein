package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;

public interface GlobalAuthSessionDao extends BaseDao<GlobalAuthSession> {

    List<GlobalAuthSession> findByTicketIdsAndTenant(List<Long> ticketIds, GlobalAuthTenant globalAuthTenant);

}
