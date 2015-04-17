package com.latticeengines.admin.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.dao.BardJamsRequestDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;

@Component
public class BardJamsRequestDaoImpl extends BaseDaoImpl<BardJamsTenant> implements BardJamsRequestDao {

    @Override
    protected Class<BardJamsTenant> getEntityClass() {
        return BardJamsTenant.class;
    }

}