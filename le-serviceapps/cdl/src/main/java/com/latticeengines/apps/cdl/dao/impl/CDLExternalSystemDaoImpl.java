package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.CDLExternalSystemDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;

@Component("cdlExternalSystemDao")
public class CDLExternalSystemDaoImpl extends BaseDaoImpl<CDLExternalSystem> implements CDLExternalSystemDao {
    @Override
    protected Class<CDLExternalSystem> getEntityClass() {
        return CDLExternalSystem.class;
    }
}
