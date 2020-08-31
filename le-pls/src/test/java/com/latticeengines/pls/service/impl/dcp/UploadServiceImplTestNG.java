package com.latticeengines.pls.service.impl.dcp;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadJobDetails;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.pls.functionalframework.DCPFunctionalTestNGBase;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

public class UploadServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private UploadService uploadService;

    private static final String RUNNING_UPLOAD = "Upload_running";
    private static final String FINISHED_UPLOAD = "Upload_finished";
    private static final String RUNNING_APPLICATION = "fake_application_running";
    private static final String COMPLETED_APPLICATION = "fake_application_completed";


    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithOneGATenant();
        SourceProxy sourceProxy = mock(SourceProxy.class);
        UploadProxy uploadProxy = mock(UploadProxy.class);
        WorkflowJobService workflowJobService = mock(WorkflowJobService.class);
        when(uploadProxy.getUploadByUploadId(anyString(), anyString(), anyBoolean())).thenAnswer(invocation -> getUploadDetails(invocation.getArgument(1)));
        when(sourceProxy.getSource(anyString(), anyString())).thenAnswer(invocation -> getSource(invocation.getArgument(1)));
        when(workflowJobService.findByApplicationId(anyString())).thenAnswer(invocation -> getJob(invocation.getArgument(0)));
        ReflectionTestUtils.setField(uploadService, "sourceProxy", sourceProxy);
        ReflectionTestUtils.setField(uploadService, "uploadProxy", uploadProxy);
        ReflectionTestUtils.setField(uploadService, "workflowJobService", workflowJobService);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "functional")
    public void testGetJobDetails() {
        UploadJobDetails runningJobDetails = uploadService.getJobDetailsByUploadId(RUNNING_UPLOAD);
        UploadJobDetails completeJobDetails = uploadService.getJobDetailsByUploadId(FINISHED_UPLOAD);
        Assert.assertNotNull(runningJobDetails);
        Assert.assertNotNull(runningJobDetails.getUploadJobSteps());
        Assert.assertEquals(runningJobDetails.getUploadJobSteps().size(), 1);
        Assert.assertNotNull(runningJobDetails.getCurrentStep());
        Assert.assertNull(runningJobDetails.getCurrentStep().getEndTimestamp());

        Assert.assertNotNull(completeJobDetails);
        Assert.assertNotNull(completeJobDetails.getUploadJobSteps());
        Assert.assertEquals(completeJobDetails.getUploadJobSteps().size(), 3);
        Assert.assertNull(completeJobDetails.getCurrentStep());
    }

    private UploadDetails getUploadDetails(String uploadId) {
        UploadDiagnostics uploadDiagnostics = new UploadDiagnostics();
        UploadDetails uploadDetails = new UploadDetails();
        if (RUNNING_UPLOAD.equals(uploadId)) {
            uploadDiagnostics.setApplicationId(RUNNING_APPLICATION);
            uploadDetails.setProgressPercentage(66.6);
            uploadDetails.setStatus(Upload.Status.IMPORT_STARTED);
        } else if (FINISHED_UPLOAD.equals(uploadId)) {
            uploadDiagnostics.setApplicationId(COMPLETED_APPLICATION);
            uploadDetails.setProgressPercentage(100.0);
            uploadDetails.setStatus(Upload.Status.FINISHED);
        } else {
            uploadDiagnostics.setApplicationId("fake_application_1234");
            uploadDetails.setProgressPercentage(99.9);
            uploadDetails.setStatus(Upload.Status.ERROR);
        }

        uploadDetails.setUploadId(uploadId);
        uploadDetails.setDisplayName("DisplayName_" + uploadId);
        uploadDetails.setSourceId("SourceId_" + uploadId);
        uploadDetails.setUploadDiagnostics(uploadDiagnostics);
        uploadDetails.setUploadId(uploadId);
        return uploadDetails;
    }

    private Source getSource(String sourceId) {
        Source source = new Source();
        source.setSourceId(sourceId);
        source.setSourceId("DisplayName_" + sourceId);
        return source;
    }

    private Job getJob(String applicationId) {
        Job job = new Job();
        List<JobStep> stepList = new ArrayList<>();
        if (RUNNING_APPLICATION.equals(applicationId)) {
            stepList.add(getJobStep(0, true));
            stepList.add(getJobStep(1, true));
            stepList.add(getJobStep(2, false));
        } else if (COMPLETED_APPLICATION.equals(applicationId)) {
            stepList.add(getJobStep(0, true));
            stepList.add(getJobStep(1, true));
            stepList.add(getJobStep(2, true));
            stepList.add(getJobStep(3, true));
            stepList.add(getJobStep(4, true));
            stepList.add(getJobStep(5, true));
        }
        job.setSteps(stepList);
        return job;
    }

    private JobStep getJobStep(int index, boolean complete) {
        JobStep jobStep = new JobStep();
        switch (index) {
            case 0:
            case 1:
            case 2:
                jobStep.setName("Ingestion");
                jobStep.setDescription("Ingestion");
                break;
            case 3:
                jobStep.setName("Match_Append");
                jobStep.setDescription("Match_Append");
                break;
            case 4:
            case 5:
                jobStep.setName("Analysis");
                jobStep.setDescription("Analysis");
                break;
            default:
                throw new IllegalArgumentException("Step not supported.");
        }
        jobStep.setStartTimestamp(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10 - index)));
        if (complete) {
            jobStep.setEndTimestamp(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10 - index + 1)));
        }
        return jobStep;
    }
}
