package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.EventOutgoingDao;
import com.latticeengines.dataplatform.entitymanager.EventOutgoingEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.EventOutgoing;

@Component("eventOutgoingEntityMgr")
public class EventOutgoingEntityMgrImpl extends BaseEntityMgrImpl<EventOutgoing> implements EventOutgoingEntityMgr {

    @Autowired
    private EventOutgoingDao eventOutgoingDao;
    
    public EventOutgoingEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<EventOutgoing> getDao() {
        return eventOutgoingDao;
    }  

}
