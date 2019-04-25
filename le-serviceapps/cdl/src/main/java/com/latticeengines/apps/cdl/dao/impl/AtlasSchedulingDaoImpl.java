package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AtlasSchedulingDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

@Component("atlasSchedulingDao")
public class AtlasSchedulingDaoImpl extends BaseDaoImpl<AtlasScheduling> implements AtlasSchedulingDao {
    @Override
    protected Class<AtlasScheduling> getEntityClass() {
        return AtlasScheduling.class;
    }
}
