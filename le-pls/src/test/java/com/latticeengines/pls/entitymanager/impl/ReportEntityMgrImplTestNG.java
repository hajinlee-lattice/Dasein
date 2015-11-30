package com.latticeengines.pls.entitymanager.impl;

import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.pls.entitymanager.ReportEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

public class ReportEntityMgrImplTestNG extends PlsFunctionalTestNGBase {
    @Autowired
    private ReportEntityMgr reportEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private String guid;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupUsers();
        cleanupDB();
    }

    @BeforeMethod(groups = "functional")
    public void setupContext() {
        setupSecurityContext(mainTestingTenant);
    }

    @Test(groups = "functional")
    public void testCreateReport() {
        Report report = new Report();
        report.setPurpose("TEST");
        report.setJson("{\"poopy\":\"true\"}");
        report.setIsOutOfDate(false);
        reportEntityMgr.create(report);
        guid = report.getGuid();

        Report indb = reportEntityMgr.findByGuid(report.getGuid());
        Assert.assertNotNull(indb);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateReport")
    public void testUpdateReport() {
        Report report = new Report();
        report.setGuid(guid);
        report.setPurpose("TEST");
        report.setJson("{\"poopy\":\"true\"}");
        report.setIsOutOfDate(true);
        reportEntityMgr.createOrUpdate(report);

        Report indb = reportEntityMgr.findByGuid(report.getGuid());
        Assert.assertNotNull(indb);
        Assert.assertTrue(indb.isOutOfDate());
    }


    @Test(groups = "functional", dependsOnMethods = "testUpdateReport")
    public void testDeleteReport() {
        reportEntityMgr.delete(reportEntityMgr.findByGuid(guid));

        Report indb = reportEntityMgr.findByGuid(guid);
        Assert.assertNull(indb);
    }

    protected void cleanupDB() {
        setupSecurityContext(mainTestingTenant);
        List<Report> reports = reportEntityMgr.findAll();
        for (Report report : reports) {
            if (report.getPurpose().equals("TEST")) {
                reportEntityMgr.delete(report);
            }
        }
    }
}
