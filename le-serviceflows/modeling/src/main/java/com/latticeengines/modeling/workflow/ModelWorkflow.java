package com.latticeengines.modeling.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.MatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.PersistDataRules;
import com.latticeengines.modeling.workflow.steps.RemediateDataRules;
import com.latticeengines.modeling.workflow.steps.modeling.CreateModel;
import com.latticeengines.modeling.workflow.steps.modeling.CreateNote;
import com.latticeengines.modeling.workflow.steps.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.modeling.workflow.steps.modeling.InvokeDataScienceAnalysis;
import com.latticeengines.modeling.workflow.steps.modeling.Profile;
import com.latticeengines.modeling.workflow.steps.modeling.ReviewModel;
import com.latticeengines.modeling.workflow.steps.modeling.Sample;
import com.latticeengines.modeling.workflow.steps.modeling.SetMatchSelection;
import com.latticeengines.modeling.workflow.steps.modeling.WriteMetadataFiles;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelWorkflow")
public class ModelWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Autowired
    private Sample sample;

    @Autowired
    private ExportData exportData;

    @Autowired
    private SetMatchSelection setMatchSelection;

    @Autowired
    private WriteMetadataFiles writeMetadataFiles;

    @Autowired
    private Profile profile;

    @Autowired
    private ReviewModel reviewModel;

    @Autowired
    private CreateModel createModel;

    @Autowired
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

    @Autowired
    private CreateNote createNote;

    @Autowired
    private PersistDataRules persistDataRules;

    @Autowired
    private RemediateDataRules remediateDataRules;

    @Autowired
    InvokeDataScienceAnalysis invokeDataScienceAnalysis;

    @Bean
    public Job modelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(sample) //
                .next(exportData) //
                .next(setMatchSelection) //
                .next(writeMetadataFiles) //
                .next(profile) //
                .next(reviewModel) //
                .next(remediateDataRules) //
                .next(writeMetadataFiles) //
                .next(createModel) //
                .next(downloadAndProcessModelSummaries) //
                .next(createNote).next(persistDataRules).next(invokeDataScienceAnalysis)//
                .build();
    }
}
