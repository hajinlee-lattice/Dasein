package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.ConsolidateDataWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("consolidateDataWorkflow")
public class ConsolidateDataWorkflow extends AbstractWorkflow<ConsolidateDataWorkflowConfiguration> {

    @Autowired
    private ConsolidateAccountWrapper consolidateAccountWrapper;

    @Autowired
    private ConsolidateContactWrapper consolidateContactWrapper;

    @Autowired
    private ConsolidateTransactionWrapper consolidateTransactionWrapper;

    
    @Bean
    public Job consolidateDataWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(consolidateAccountWrapper) //
                .next(consolidateContactWrapper) //
//                .next(consolidateTransactionWrapper) //
                .build();
    }
}
