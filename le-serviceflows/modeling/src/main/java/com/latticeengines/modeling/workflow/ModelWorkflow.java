package com.latticeengines.modeling.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
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
@Lazy
public class ModelWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Inject
    private Sample sample;

    @Inject
    private ExportData exportData;

    @Inject
    private SetMatchSelection setMatchSelection;

    @Inject
    private WriteMetadataFiles writeMetadataFiles;

    @Inject
    private Profile profile;

    @Inject
    private ReviewModel reviewModel;

    @Inject
    private CreateModel createModel;

    @Inject
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

    @Inject
    private CreateNote createNote;

    @Inject
    private PersistDataRules persistDataRules;

    @Inject
    private RemediateDataRules remediateDataRules;

    @Inject
    InvokeDataScienceAnalysis invokeDataScienceAnalysis;

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
