package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.CDLJobDetailDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;

@Component("cdlJobDetailDao")
public class CDLJobDetailDaoImpl extends BaseDaoImpl<CDLJobDetail> implements CDLJobDetailDao {
    @Override
    protected Class<CDLJobDetail> getEntityClass() {
        return CDLJobDetail.class;
    }
}
