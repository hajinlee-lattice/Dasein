package com.latticeengines.security.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.GlobalAuthSession;

public interface GlobalAuthSessionEntityMgr extends BaseEntityMgr<GlobalAuthSession> {

    GlobalAuthSession findByTicketId(Long ticketId);

}
