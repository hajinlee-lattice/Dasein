package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.PublishToElasticSearchStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishElasticSearchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportToElasticSearch;
import com.latticeengines.serviceflows.workflow.export.ImportTableRoleFromS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishElasticSearchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishElasticSearchWorkflow extends AbstractWorkflow<PublishElasticSearchWorkflowConfiguration> {

    //Publish table to hdfs
    @Inject
    private ImportTableRoleFromS3 importTableRoleFromS3;
    @Inject
    private PublishToElasticSearchStep publishToElasticSearchStep;
    //send table to es
    @Inject
    private ExportToElasticSearch exportToElasticSearch;


    @Override
    public Workflow defineWorkflow(PublishElasticSearchWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig) //
                .next(importTableRoleFromS3) //
                .next(publishToElasticSearchStep) //
                .next(exportToElasticSearch) //
                .build();
    }
}
