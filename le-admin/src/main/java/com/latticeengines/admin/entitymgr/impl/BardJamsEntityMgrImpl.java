package com.latticeengines.admin.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.admin.dao.BardJamsRequestDao;
import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;

@Component("bardJamsEntityMgr")
public class BardJamsEntityMgrImpl implements BardJamsEntityMgr {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(BardJamsEntityMgrImpl.class);

    @Autowired
    private BardJamsRequestDao bardJamsRequestDao;

    @Override
    @Transactional(value = "bardJamsRequest")
    public void create(BardJamsTenant request) {
        bardJamsRequestDao.create(request);
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public void update(BardJamsTenant request) {
        bardJamsRequestDao.update(request);
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public void delete(BardJamsTenant request) {
        bardJamsRequestDao.delete(request);
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public BardJamsTenant findByKey(BardJamsTenant request) {
        return bardJamsRequestDao.findByKey(request);
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public BardJamsTenant findByTenant(BardJamsTenant request) {
        return bardJamsRequestDao.findByTenant(request.getTenant());
    }

    @Override
    @Transactional(value = "bardJamsRequest")
    public BardJamsTenant findByTenant(String tenant) {
        return bardJamsRequestDao.findByTenant(tenant);
    }
}
