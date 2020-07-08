package com.latticeengines.apps.dcp.end2end;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;

public class DCPDataReportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private DataReportProxy reportProxy;

    @BeforeClass(groups = { "deployment" })
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testDataReport() {
        DCPReportRequest request = new DCPReportRequest();
        request.setLevel(DataReportRecord.Level.Project);
        request.setMode(DataReportMode.UPDATE);
        request.setTenant(mainCustomerSpace);

        ApplicationId appId = reportProxy.rollupDataReport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }
}
