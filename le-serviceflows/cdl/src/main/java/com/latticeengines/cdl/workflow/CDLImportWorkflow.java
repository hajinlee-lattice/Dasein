package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlImportWorkflow")
public class CDLImportWorkflow extends AbstractWorkflow<CDLImportWorkflowConfiguration> {

    @Autowired
    private ImportAccountData importAccountData;

    @Autowired
    private ImportContactData importContactData;
    
    @Autowired
    private ImportTimeSeriesData importTimeSeriesData;
    
    @Autowired
    private ImportCategoryData importCategoryData;

    @Bean
    public Job cdlImportWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(importAccountData) //
                .next(importContactData) //
                .next(importTimeSeriesData) //
                .next(importCategoryData) //
                .build();
    }

}
