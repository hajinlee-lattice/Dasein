package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.DashboardEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.DashboardWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.DashboardReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dashboardEntityMgr")
public class DashboardEntityMgrImpl extends JpaEntityMgrRepositoryImpl<Dashboard, Long> implements DashboardEntityMgr {

    @Inject
    private DashboardReaderRepository readerRepository;
    @Inject
    private DashboardWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Dashboard findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Dashboard findByTenantAndName(Tenant tenant, String name) {
        return readerRepository.findByNameAndTenant(name, tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Dashboard> findAllByTenant(Tenant tenant) {
        return readerRepository.findAllByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<Dashboard, Long> getRepository() {
        return writerRepository;
    }
}
