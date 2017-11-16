package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.CdlMatchAndModelWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.CreateModel;
import com.latticeengines.serviceflows.workflow.modeling.CreateNote;
import com.latticeengines.serviceflows.workflow.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.serviceflows.workflow.modeling.Profile;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.serviceflows.workflow.modeling.SetMatchSelection;
import com.latticeengines.serviceflows.workflow.modeling.WriteMetadataFiles;
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

    @Bean
    public Job modelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(sample) //
                .next(setMatchSelection) //
                .next(writeMetadataFiles) //
                .next(profile) //
                .next(createModel) //
                .next(downloadAndProcessModelSummaries) //
                .next(createNote).build();
    }
}
