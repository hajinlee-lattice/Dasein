package com.latticeengines.pls.entitymanager.impl;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.ReportDao;
import com.latticeengines.pls.entitymanager.ReportEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component("reportEntityMgr")
public class ReportEntityMgrImpl extends BaseEntityMgrImpl<Report> implements ReportEntityMgr {

    @Autowired
    private ReportDao reportDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<Report> getDao() {
        return reportDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Report findByGuid(String guid) {
        return reportDao.findByField("guid", guid);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void create(Report report) {
        Report existing = findByGuid(report.getGuid());
        if (existing != null) {
            throw new RuntimeException(String.format("Report with guid %s already exists", report.getGuid()));
        }

        initialize(report);
        getDao().create(report);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(Report report) {
        Report existing = findByGuid(report.getGuid());
        if (existing != null) {
            delete(existing);
        }

        initialize(report);
        if (existing != null) {
            report.setGuid(existing.getGuid());
        }
        getDao().create(report);
    }

    private void initialize(Report report) {
        Tenant tenant = tenantEntityMgr.findByTenantId(SecurityContextUtils.getTenant().getId());
        report.setPid(null);
        report.setGuid(UUID.randomUUID().toString());
        report.setTenant(tenant);
    }
}
