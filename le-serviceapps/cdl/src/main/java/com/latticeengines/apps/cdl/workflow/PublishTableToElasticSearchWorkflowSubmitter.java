package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishTableToElasticSearchWorkflowConfiguration;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;

@Component
public class PublishTableToElasticSearchWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(PublishTableToElasticSearchWorkflowSubmitter.class);

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ElasticSearchService elasticSearchService;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace,
                                PublishTableToESRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        log.info("publish table to elastic search for customer {} with workflow {}", customerSpace, pidWrapper.getPid());


        setElasticSearchConfigForEmpty(request);
        PublishTableToElasticSearchWorkflowConfiguration configuration =
                new PublishTableToElasticSearchWorkflowConfiguration.Builder()
                        .customer(CustomerSpace.parse(customerSpace))
                        .signature(request.getSignature())
                        .exportConfigs(request.getExportConfigs())
                        .esConfigs(request.getEsConfig())
                        .lookupIds(request.getLookupIds())
                        .build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private void setElasticSearchConfigForEmpty(PublishTableToESRequest request) {
        ElasticSearchConfig defaultEsConfig = elasticSearchService.getDefaultElasticSearchConfig();
        if (request.getEsConfig() == null) {
            request.setEsConfig(defaultEsConfig);
            return ;
        }
        ElasticSearchConfig esConfig = request.getEsConfig();
        if (esConfig.getDynamic() == null) {
            esConfig.setDynamic(defaultEsConfig.getDynamic());
        }
        if (esConfig.getRefreshInterval() == null) {
            esConfig.setRefreshInterval(defaultEsConfig.getRefreshInterval());
        }
        if (esConfig.getReplicas() == null) {
            esConfig.setReplicas(defaultEsConfig.getReplicas());
        }
        if (esConfig.getShards() == null) {
            esConfig.setShards(defaultEsConfig.getShards());
        }
        if (esConfig.getHttpScheme() == null) {
            esConfig.setHttpScheme(defaultEsConfig.getHttpScheme());
        }

    }

}
