package com.latticeengines.datacloud.etl.orchestration.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.orchestration.dao.OrchestrationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;

@Component("orchestrationDao")
public class OrchestrationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Orchestration>
        implements OrchestrationDao {
    @Override
    protected Class<Orchestration> getEntityClass() {
        return Orchestration.class;
    }
}
