package com.latticeengines.workflowapi.service.impl;

import static com.latticeengines.domain.exposed.metadata.datastore.DataUnit.DataFormat.AVRO;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;
import com.latticeengines.spark.exposed.job.common.CopyJob;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.SparkWorkflowService;

public class SparkWorkflowServiceImplTestNG extends WorkflowApiFunctionalTestNGBase  {

    private static final Logger log = LoggerFactory.getLogger(SparkWorkflowServiceImplTestNG.class);
    protected static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/customevent";

    @Inject
    private SparkWorkflowService sparkWorkflowService;

    @Test(groups = "functional")
    public void testRunSparkWorkflow() {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());

        HdfsDataUnit inputUnit = uploadInput(customerSpace);
        CopyConfig jobConfig = new CopyConfig();
        jobConfig.setInput(Collections.singletonList(inputUnit));

        RunSparkWorkflowRequest request = new RunSparkWorkflowRequest();
        request.setSparkJobConfig(jobConfig);
        request.setSparkJobClz(CopyJob.class.getSimpleName());

        AppSubmission appSubmission = sparkWorkflowService.submitSparkJob(customerSpace.toString(), request);
        String applicationId = appSubmission.getApplicationIds().get(0);
        log.info("applicationId={}", applicationId);
        JobStatus jobStatus = waitForWorkflowStatus(applicationId, false);
        Assert.assertEquals(jobStatus, JobStatus.COMPLETED);

    }

    private HdfsDataUnit uploadInput(CustomerSpace customerSpace) {
        try {
            String hdfsDir = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
            if (!HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
                HdfsUtils.mkdir(yarnConfiguration, hdfsDir);
            }
            hdfsDir += "/Input";
            String fullLocalPath = RESOURCE_BASE + "/accountTable.avro";
            InputStream fileStream = ClassLoader.getSystemResourceAsStream(fullLocalPath);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, hdfsDir + "/part-00000.avro");

            HdfsDataUnit dataUnit = new HdfsDataUnit();
            dataUnit.setPath(hdfsDir);
            dataUnit.setName("Input");
            dataUnit.setDataFormat(AVRO);
            return dataUnit;
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload data");
        }
    }

}
