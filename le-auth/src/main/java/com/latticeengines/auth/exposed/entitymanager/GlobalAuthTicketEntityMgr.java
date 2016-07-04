package com.latticeengines.auth.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;

public interface GlobalAuthTicketEntityMgr extends BaseEntityMgr<GlobalAuthTicket> {

    GlobalAuthTicket findByTicket(String ticket);

}
