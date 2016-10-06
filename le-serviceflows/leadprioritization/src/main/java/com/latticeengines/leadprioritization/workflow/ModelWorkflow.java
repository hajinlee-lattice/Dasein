package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.modeling.CreateModel;
import com.latticeengines.serviceflows.workflow.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.serviceflows.workflow.modeling.Profile;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.serviceflows.workflow.modeling.SetMatchSelection;
import com.latticeengines.serviceflows.workflow.modeling.WriteMetadataFiles;
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
    private CreateModel createModel;

    @Autowired
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

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
                .build();
    }
}
