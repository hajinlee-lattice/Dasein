package com.latticeengines.workflow.exposed.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.exposed.dao.KeyValueDao;
import com.latticeengines.workflow.exposed.dao.ReportDao;
import com.latticeengines.workflow.exposed.entitymgr.ReportEntityMgr;

@Component("reportEntityMgr")
public class ReportEntityMgrImpl extends BaseEntityMgrImpl<Report> implements ReportEntityMgr {

    @Autowired
    private KeyValueDao keyValueDao;

    @Autowired
    private ReportDao reportDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<Report> getDao() {
        return reportDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Report findByName(String name) {
        Report report = reportDao.findByField("name", name);

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
        keyValueDao.create(json);
        getDao().create(report);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void create(Report report) {
        internalCreate(report);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Report> findAll() {
        List<Report> reports = super.findAll();
        for (Report report : reports) {
            HibernateUtils.inflateDetails(report.getJson());
        }
        return reports;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Report> getAll() {
        return findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(Report report) {
        KeyValue kv = report.getJson();
        super.delete(report);
        keyValueDao.delete(kv);

    }

}
