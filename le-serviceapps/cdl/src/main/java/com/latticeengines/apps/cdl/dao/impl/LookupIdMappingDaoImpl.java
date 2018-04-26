package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.LookupIdMappingDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

@Component("lookupIdMappingDao")
public class LookupIdMappingDaoImpl extends BaseDaoImpl<LookupIdMap> implements LookupIdMappingDao {

    @Override
    protected Class<LookupIdMap> getEntityClass() {
        return LookupIdMap.class;
    }

}
