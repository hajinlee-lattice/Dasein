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
import com.latticeengines.domain.exposed.elasticsearch.PublishESRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishElasticSearchWorkflowConfiguration;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;

@Component
public class PublishTableRoleToESWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(PublishTableRoleToESWorkflowSubmitter.class);

    @Inject
    private DataCollectionService dataCollectionService;
    @Inject
    private ElasticSearchService elasticSearchService;

    @WithWorkflowJobPid
    public ApplicationId submitPublishES(@NotNull String customerSpace, @NotNull PublishESRequest publishESRequest,
                                         @NotNull WorkflowPidWrapper pidWrapper) {
        log.info(String.format("PublishESWorkflowJob created for customer=%s with pid=%s", customerSpace,
                pidWrapper.getPid()));
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        if (publishESRequest.getOriginTenant() == null) {
            publishESRequest.setOriginTenant(customerSpace);
        }
        if (publishESRequest.getVersion() == null) {
            publishESRequest.setVersion(dataCollectionService.getActiveVersion(publishESRequest.getOriginTenant()));
        }
        PublishElasticSearchWorkflowConfiguration configuration = new PublishElasticSearchWorkflowConfiguration.Builder()
                .customer(CustomerSpace.parse(customerSpace))
                .tableRoles(publishESRequest.getTableRoles())
                .version(publishESRequest.getVersion())
                .esConfig(generateESConfig(publishESRequest))
                .originalTenant(publishESRequest.getOriginTenant())
                .build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());

    }

    private ElasticSearchConfig generateESConfig(PublishESRequest request) {
        ElasticSearchConfig esConfig;
        ElasticSearchConfig defaultEsConfig = elasticSearchService.getDefaultElasticSearchConfig();
        if (request.getEsConfig() == null) {
            return defaultEsConfig;
        }
        esConfig = request.getEsConfig();
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
        return esConfig;
    }
}
