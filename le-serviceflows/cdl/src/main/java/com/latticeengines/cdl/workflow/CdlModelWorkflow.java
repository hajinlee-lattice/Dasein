package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.CdlMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.modeling.CreateModel;
import com.latticeengines.modeling.workflow.steps.modeling.CreateNote;
import com.latticeengines.modeling.workflow.steps.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.modeling.workflow.steps.modeling.Profile;
import com.latticeengines.modeling.workflow.steps.modeling.Sample;
import com.latticeengines.modeling.workflow.steps.modeling.SetMatchSelection;
import com.latticeengines.modeling.workflow.steps.modeling.WriteMetadataFiles;
import com.latticeengines.serviceflows.workflow.export.ExportData;

import com.latticeengines.modeling.workflow.steps.modeling.InvokeDataScienceAnalysis;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlModelWorkflow")
public class CdlModelWorkflow extends AbstractWorkflow<CdlMatchAndModelWorkflowConfiguration> {

    @Autowired
    private Sample sample;

    @Autowired
    private SetMatchSelection setMatchSelection;

    @Autowired
    private WriteMetadataFiles writeMetadataFiles;

    @Autowired
    private Profile profile;

    @Autowired
    private CreateModel createModel;

    @Autowired
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

    @Autowired
    private CreateNote createNote;

    @Autowired
    private InvokeDataScienceAnalysis invokeDataScienceAnalysis;

    @Autowired
    private ExportData exportData;

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
                .next(createModel) //
                .next(downloadAndProcessModelSummaries) //
                .next(createNote)
                .next(invokeDataScienceAnalysis)
                .build();
    }

}
