package com.latticeengines.datacloud.workflow.match.steps;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchCommitter;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PublishEntityMatchStagingConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Lazy
@Component("publishEntityMatchStaging")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishEntityMatchStaging extends BaseWorkflowStep<PublishEntityMatchStagingConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PublishEntityMatchStaging.class);

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Inject
    private EntityMatchCommitter entityMatchCommitter;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void execute() {
        Tenant tenant = getTenant(CustomerSpace.parse(configuration.getCustomerSpace()));
        // clear version cache and overwrite config
        entityMatchVersionService.invalidateCache(tenant);
        overwriteEntityMatchConfiguration();

        try {
            acquireLockAndFailOpen();
            // publish from staging to serving
            configuration.getEntities().forEach(entity -> commitWithCommitter(tenant, entity.name()));
        } finally {
            EntityMatchUtils.unlockCommitStep();
        }
    }

    private void acquireLockAndFailOpen() {
        try (PerformanceTimer time = new PerformanceTimer("acquiring commit entity match staging lock")) {
            boolean success = EntityMatchUtils.lockCommitStep();
            if (!success) {
                log.warn("Failed to acquire lock for commit step. Continue (fail open) for now.");
            } else {
                log.info("Lock for commit step acquired successfully");
            }
        }
    }

    private void commitWithCommitter(Tenant tenant, String entity) {
        Map<EntityMatchEnvironment, Integer> versionMap = configuration.getVersions();
        log.info("Committing entity {} from staging to serving, versions = {}", entity, versionMap);
        try {
            EntityPublishStatistics stats = entityMatchCommitter.commit(entity, tenant, null, versionMap);
            log.info("Entity {} committed. nSeeds={}, nLookups={}, nlookupNotInStaging={}", entity,
                    stats.getSeedCount(), stats.getLookupCount(), stats.getNotInStagingLookupCount());
        } catch (Exception e) {
            if (versionMap != null) {
                // Increase next version, avoid next PA reuse current next version number.
                int nextVersion = entityMatchVersionService.bumpNextVersion(SERVING, tenant);
                log.info("entity {} commit failed, the next version be changed to {}", entity, nextVersion);
            }
            throw e;
        }
    }

    private void overwriteEntityMatchConfiguration() {
        EntityMatchConfiguration emConfig = configuration.getEntityMatchConfiguration();
        if (emConfig != null) {
            log.info("Overriding entity match configuration {}", JsonUtils.serialize(emConfig));
            EntityMatchUtils.overwriteWithConfiguration(entityMatchConfigurationService, emConfig);
        }
    }

    private Tenant getTenant(@NotNull CustomerSpace customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
        Preconditions.checkNotNull(tenant, String.format("tenant %s does not exist", customerSpace.toString()));
        return EntityMatchUtils.newStandardizedTenant(tenant);
    }
}
