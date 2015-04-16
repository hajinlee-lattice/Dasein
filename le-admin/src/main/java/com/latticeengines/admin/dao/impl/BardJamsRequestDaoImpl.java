package com.latticeengines.admin.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.dao.BardJamsRequestDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.admin.BardJamsTenants;

@Component
public class BardJamsRequestDaoImpl extends BaseDaoImpl<BardJamsTenants> implements BardJamsRequestDao {

    @Override
    protected Class<BardJamsTenants> getEntityClass() {
        return BardJamsTenants.class;
    }

}