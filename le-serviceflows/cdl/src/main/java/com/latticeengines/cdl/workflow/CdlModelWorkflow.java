package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.CdlModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.PersistDataRules;
import com.latticeengines.modeling.workflow.steps.RemediateDataRules;
import com.latticeengines.modeling.workflow.steps.modeling.CreateModel;
import com.latticeengines.modeling.workflow.steps.modeling.CreateNote;
import com.latticeengines.modeling.workflow.steps.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.modeling.workflow.steps.modeling.EventCounter;
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

@Component("cdlModelWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CdlModelWorkflow extends AbstractWorkflow<CdlModelWorkflowConfiguration> {

    @Inject
    private EventCounter eventCounter;

    @Inject
    private Sample sample;

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
    private InvokeDataScienceAnalysis invokeDataScienceAnalysis;

    @Autowired
    private ExportData exportData;

    @Override
    public Workflow defineWorkflow(CdlModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(eventCounter) //
                .next(sample) //
                .next(exportData) //
                .next(setMatchSelection) //
                .next(writeMetadataFiles) //
                .next(profile) //
                .next(reviewModel) //
                .next(remediateDataRules) //
                .next(createModel) //
                .next(downloadAndProcessModelSummaries) //
                .next(createNote) //
                .next(persistDataRules) //
                .next(invokeDataScienceAnalysis) //
                .build();
    }

}
