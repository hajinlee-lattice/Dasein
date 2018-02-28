package com.latticeengines.datacloud.etl.purge.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.purge.dao.PurgeStrategyDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;

@Component("purgeStrategyDao")
public class PurgeStrategyDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<PurgeStrategy>
        implements PurgeStrategyDao {
    @Override
    protected Class<PurgeStrategy> getEntityClass() {
        return PurgeStrategy.class;
    }
}
