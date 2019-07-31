package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ImportMigrateTrackingDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

@Component("importMigrateTrackingDao")
public class ImportMigrateTrackingDaoImpl extends BaseDaoImpl<ImportMigrateTracking> implements ImportMigrateTrackingDao {
    @Override
    protected Class<ImportMigrateTracking> getEntityClass() {
        return ImportMigrateTracking.class;
    }
}
