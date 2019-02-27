package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.dao.ArtifactDao;
import com.latticeengines.metadata.dao.ModuleDao;
import com.latticeengines.metadata.entitymgr.ArtifactEntityMgr;

@Component("artifactEntityMgr")
public class ArtifactEntityMgrImpl extends BaseEntityMgrImpl<Artifact> implements ArtifactEntityMgr {

    @Autowired
    private ArtifactDao artifactDao;

    @Autowired
    private ModuleDao moduleDao;

    @Override
    public BaseDao<Artifact> getDao() {
        return artifactDao;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void create(Artifact artifact) {
        Module module = moduleDao.findByField("NAME", artifact.getModule().getName());
        Tenant tenant = MultiTenantContext.getTenant();
        if (module == null) {
            module = new Module();
            module.setName(artifact.getModule().getName());
            module.setTenant(tenant);
            moduleDao.create(module);
        }
        artifact.setModule(module);
        artifact.setTenantId(tenant.getPid());
        artifactDao.create(artifact);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Artifact> findAll() {
        return super.findAll();
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Artifact findByPath(String path) {
        return artifactDao.findByField("PATH", path);
    }

}
