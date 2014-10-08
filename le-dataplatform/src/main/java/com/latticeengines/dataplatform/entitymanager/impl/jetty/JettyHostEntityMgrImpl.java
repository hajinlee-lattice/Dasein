package com.latticeengines.dataplatform.entitymanager.impl.jetty;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.jetty.JettyHostDao;
import com.latticeengines.dataplatform.entitymanager.impl.BaseEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.jetty.JettyHostEntityMgr;
import com.latticeengines.domain.exposed.jetty.JettyHost;
import com.latticeengines.domain.exposed.jetty.JettyJob;

@Component("jettyHostEntityMgr")
public class JettyHostEntityMgrImpl extends BaseEntityMgrImpl<JettyHost> implements JettyHostEntityMgr {

    @Autowired
    private JettyHostDao jettyHostDao;

    @Override
    public BaseDao<JettyHost> getDao() {
        return jettyHostDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<JettyHost> findByJettyJob(JettyJob jettyJob) {
        return jettyHostDao.findByJettyJob(jettyJob);
    }
}
