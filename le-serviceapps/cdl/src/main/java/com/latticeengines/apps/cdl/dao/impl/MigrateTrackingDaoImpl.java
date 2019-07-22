package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.MigrateTrackingDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

@Component("migrateTrackingDao")
public class MigrateTrackingDaoImpl extends BaseDaoImpl<MigrateTracking> implements MigrateTrackingDao {
    @Override
    protected Class<MigrateTracking> getEntityClass() {
        return MigrateTracking.class;
    }
}
