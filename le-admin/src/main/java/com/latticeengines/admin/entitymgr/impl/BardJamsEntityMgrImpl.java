package com.latticeengines.admin.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.admin.dao.BardJamsRequestDao;
import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.domain.exposed.admin.BardJamsTenants;

@Component("bardJamsEntityMgr")
public class BardJamsEntityMgrImpl implements BardJamsEntityMgr {
    private static final Log log = LogFactory.getLog(BardJamsEntityMgrImpl.class);

    @Autowired
    private BardJamsRequestDao bardJamsRequestDao;

    @Override
    @Transactional(value = "bardJamsRequest")
    public void create(BardJamsTenants request) {
        bardJamsRequestDao.create(request);
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public void delete(BardJamsTenants request) {
        bardJamsRequestDao.delete(request);
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public BardJamsTenants findByKey(BardJamsTenants request) {
        return bardJamsRequestDao.findByKey(request);
    }
}
