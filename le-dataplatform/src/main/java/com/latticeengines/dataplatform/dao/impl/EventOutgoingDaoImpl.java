package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.EventOutgoingDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.EventOutgoing;

@Component("eventOutgoingDao")
public class EventOutgoingDaoImpl extends BaseDaoImpl<EventOutgoing> implements EventOutgoingDao {

    public EventOutgoingDaoImpl() {
        super();
    }

    @Override
    public EventOutgoing deserialize(String id, String content) {
        // TODO Auto-generated method stub
        return null;
    }

}