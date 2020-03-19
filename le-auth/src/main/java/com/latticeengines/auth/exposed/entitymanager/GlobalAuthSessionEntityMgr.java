package com.latticeengines.auth.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;

public interface GlobalAuthSessionEntityMgr extends BaseEntityMgr<GlobalAuthSession> {

    GlobalAuthSession findByTicketId(Long ticketId);

    GlobalAuthSession findByTicketIdAndTenantIdAndUserId(Long ticketId, Long tenantId, Long userId);
}
