package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.CdlMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.modeling.CreateModel;
import com.latticeengines.modeling.workflow.steps.modeling.CreateNote;
import com.latticeengines.modeling.workflow.steps.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.modeling.workflow.steps.modeling.InvokeDataScienceAnalysis;
import com.latticeengines.modeling.workflow.steps.modeling.Profile;
import com.latticeengines.modeling.workflow.steps.modeling.Sample;
import com.latticeengines.modeling.workflow.steps.modeling.SetMatchSelection;
import com.latticeengines.modeling.workflow.steps.modeling.WriteMetadataFiles;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlModelWorkflow")
@Lazy
public class CdlModelWorkflow extends AbstractWorkflow<CdlMatchAndModelWorkflowConfiguration> {

    @Inject
    private Sample sample;

    @Inject
    private SetMatchSelection setMatchSelection;

    @Inject
    private WriteMetadataFiles writeMetadataFiles;

    @Inject
    private Profile profile;

    @Inject
    private CreateModel createModel;

    @Inject
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

    @Inject
    private CreateNote createNote;

    @Inject
    private InvokeDataScienceAnalysis invokeDataScienceAnalysis;

    @Autowired
    private ExportData exportData;

    @Override
    public Workflow defineWorkflow(CdlMatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder().next(sample) //
                .next(exportData) //
                .next(setMatchSelection) //
                .next(writeMetadataFiles) //
                .next(profile) //
                .next(createModel) //
                .next(downloadAndProcessModelSummaries) //
                .next(createNote).next(invokeDataScienceAnalysis).build();
    }

}
