package com.latticeengines.db.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.ReportDao;
import com.latticeengines.db.exposed.entitymgr.KeyValueEntityMgr;
import com.latticeengines.db.exposed.entitymgr.ReportEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.db.repository.ReportRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;

@Component("reportEntityMgr")
public class ReportEntityMgrImpl extends BaseEntityMgrRepositoryImpl<Report, Long> implements ReportEntityMgr {

    private final KeyValueEntityMgr keyValueEntityMgr;

    private final ReportRepository reportRepository;

    private final TenantEntityMgr tenantEntityMgr;

    private final ReportDao reportDao;

    @Inject
    public ReportEntityMgrImpl(KeyValueEntityMgr keyValueEntityMgr, ReportRepository reportRepository,
            TenantEntityMgr tenantEntityMgr, ReportDao reportDao) {
        this.keyValueEntityMgr = keyValueEntityMgr;
        this.reportRepository = reportRepository;
        this.tenantEntityMgr = tenantEntityMgr;
        this.reportDao = reportDao;
    }

    @Override
    public BaseDao<Report> getDao() {
        return reportDao;
    }

    @Override
    public BaseJpaRepository<Report, Long> getRepository() {
        return reportRepository;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Report findByName(String name) {
        Report report = reportRepository.findByName(name);

        if (report != null) {
            KeyValue json = report.getJson();
            json = HibernateUtils.inflateDetails(json);
            report.setJson(json);
        }
        return report;
    }

    private void internalCreate(Report report) {
        initialize(report);
        KeyValue json = report.getJson();
        if (json == null) {
            json = new KeyValue();
            json.setPayload("");
            report.setJson(json);
        }
        json.setTenantId(report.getTenantId());
        keyValueEntityMgr.create(json);
        getDao().create(report);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(Report report) {
        internalCreate(report);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void createOrUpdate(Report report) {
        Report existing = findByName(report.getName());
        if (existing != null) {
            delete(existing);
        }
        internalCreate(report);
    }

    private void initialize(Report report) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        report.setPid(null);
        report.setTenant(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Report> findAll() {
        List<Report> reports = super.findAll();
        for (Report report : reports) {
            HibernateUtils.inflateDetails(report.getJson());
        }
        return reports;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Report> getAll() {
        return findAll();
    }
}
