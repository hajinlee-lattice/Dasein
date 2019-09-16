package com.latticeengines.datacloud.workflow.match.steps;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PublishEntityConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * Step to publish seed/lookup table for source entity/tenant from staging environment to target tenant/environment
 */
@Component("publishEntity")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishEntity extends BaseWorkflowStep<PublishEntityConfiguration> {

    private static Logger log = LoggerFactory.getLogger(PublishEntity.class);

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Override
    public void execute() {
        log.info("Inside PublishEntity execute()");

        PublishEntityConfiguration configuration = getConfiguration();
        EntityPublishRequest request = configuration.getEntityPublishRequest();
        String entity = request.getEntity();
        // NOTE not checking whether tenant exists so that we can save/restore from
        // checkpoint
        Tenant srcTenant = request.getSrcTenant();
        Tenant destTenant = request.getDestTenant();
        EntityMatchEnvironment destEnv = request.getDestEnv();
        Boolean enableDestTTL = request.getDestTTLEnabled();

        // TODO add version support
        EntityPublishStatistics result = entityMatchInternalService.publishEntity(entity, srcTenant, destTenant,
                destEnv, enableDestTTL, null, null);
        Preconditions.checkNotNull(result);

        log.info("Publish entity={} from srcTenant={} to destTenant={},destEnv={},ttl={} successfully", entity,
                srcTenant.getId(), destTenant.getId(), destEnv, enableDestTTL);
        log.info("{} seeds and {} lookup entries are published", result.getSeedCount(), result.getLookupCount());
    }
}
