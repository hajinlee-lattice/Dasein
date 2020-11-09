package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.DashboardFilterEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.DashboardFilterWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.DashboardFilterReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dashboardFilterEntityMgr")
public class DashboardFilterEntityMgrImpl extends JpaEntityMgrRepositoryImpl<DashboardFilter, Long> implements DashboardFilterEntityMgr {

    @Inject
    private DashboardFilterReaderRepository readerRepository;
    @Inject
    private DashboardFilterWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DashboardFilter findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DashboardFilter findByNameAndDashboard(String name, Dashboard dashboard) {
        Tenant tenant = MultiTenantContext.getTenant();
        return readerRepository.findByNameAndDashboardAndTenant(name, dashboard, tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DashboardFilter> findAllByDashboard(Dashboard dashboard) {
        Tenant tenant = MultiTenantContext.getTenant();
        return readerRepository.findAllByDashboardAndTenant(dashboard, tenant);
    }

    @Override
    public List<DashboardFilter> findAllByTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return readerRepository.findAllByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<DashboardFilter, Long> getRepository() {
        return writerRepository;
    }
}
