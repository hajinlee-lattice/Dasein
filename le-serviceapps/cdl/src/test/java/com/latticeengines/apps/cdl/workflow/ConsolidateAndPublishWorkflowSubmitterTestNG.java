package com.latticeengines.apps.cdl.workflow;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class ConsolidateAndPublishWorkflowSubmitterTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateAndPublishWorkflowSubmitterTestNG.class);

    @Mock
    private WorkflowProxy workflowProxy;

    @InjectMocks
    private ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter;

    private List<Job> importJobs;

    private String customerSpace = CustomerSpace.parse("tenant").toString();

    @BeforeClass(groups = "functional")
    public void setup() {
        importJobs = generateJobs();
        MockitoAnnotations.initMocks(this);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void testGetImportJobIdsStr() {
        List<Long> jobIds = importJobs.stream().map(job -> job.getId()).collect(Collectors.toList());
        log.info("importJobIdss is " + jobIds);
        doReturn(importJobs).when(workflowProxy).getWorkflowJobs(any(String.class), nullable(List.class),
                nullable(List.class), nullable(Boolean.class), nullable(Boolean.class));
        List<Long> importJobIds = consolidateAndPublishWorkflowSubmitter.getImportJobIds(customerSpace);
        Assert.assertEquals(importJobIds.toString(), jobIds.toString());
        doReturn(Collections.emptyList()).when(workflowProxy).getWorkflowJobs(any(String.class), nullable(List.class),
                nullable(List.class), nullable(Boolean.class), nullable(Boolean.class));
        importJobIds = consolidateAndPublishWorkflowSubmitter.getImportJobIds(customerSpace);
        Assert.assertEquals(importJobIds.toString(), Collections.emptyList().toString());
    }

    private List<Job> generateJobs() {
        Job job1 = new Job();
        job1.setId(12345L);
        Job job2 = new Job();
        job2.setId(12346L);
        Job job3 = new Job();
        job3.setId(12347L);
        return Arrays.asList(job1, job2, job3);
    }
}
