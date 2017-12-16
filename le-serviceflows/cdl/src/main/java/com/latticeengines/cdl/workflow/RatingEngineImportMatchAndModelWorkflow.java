package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableFilterStep;
import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.SetCdlConfigurationForScoring;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineImportMatchAndModelWorkflow")
public class RatingEngineImportMatchAndModelWorkflow
        extends AbstractWorkflow<RatingEngineImportMatchAndModelWorkflowConfiguration> {

    @Autowired
    private CreateCdlEventTableFilterStep createCdlEventTableFilterStep;

    @Autowired
    private CreateCdlEventTableStep createCdlEventTableStep;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private CdlModelWorkflow modelWorkflow;

    @Autowired
    private SetCdlConfigurationForScoring setCdlConfigurationForScoring;

    @Autowired
    private CdlScoreWorkflow scoreWorkflow;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Bean
    public Job ratingEngineImportMatchAndModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(createCdlEventTableFilterStep) //
                .next(createCdlEventTableStep) //
                .next(matchDataCloudWorkflow) //
                .next(modelWorkflow) //
                .next(setCdlConfigurationForScoring) //
                .next(scoreWorkflow) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
