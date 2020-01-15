package com.latticeengines.datacloud.workflow.match.steps;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.EntityMatchCommitter;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * This step only runs once before switch stack.
 * It attempts to commit all supported entities, if STAGING version is bumped up.
 */
@Component("commitEntityMatch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CommitEntityMatch extends BaseWorkflowStep<CommitEntityMatchConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CommitEntityMatch.class);

    // only commit Account & Contact for now
    private static final Set<String> ENTITIES_TO_COMMIT = Sets.newHashSet( //
            BusinessEntity.Account.name(), BusinessEntity.Contact.name());

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Inject
    @Lazy
    private EntityMatchCommitter entityMatchCommitter;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        List<EntityMatchEnvironment> updatedEnvs = //
                getListObjectFromContext(NEW_ENTITY_MATCH_ENVS, EntityMatchEnvironment.class);
        if (CollectionUtils.isNotEmpty(updatedEnvs) && updatedEnvs.contains(STAGING)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(configuration.getCustomerSpace().toString());
            if (tenant == null) {
                throw new RuntimeException(
                        "Cannot find tenant with customerSpace: " + configuration.getCustomerSpace().toString());
            }

            Tenant standardizedTenant = EntityMatchUtils.newStandardizedTenant(tenant);
            entityMatchVersionService.invalidateCache(standardizedTenant);
            ENTITIES_TO_COMMIT.forEach(entity -> commitWithCommitter(standardizedTenant, entity));
        }
    }

    private void commitWithCommitter(Tenant tenant, String entity) {
        Map<EntityMatchEnvironment, Integer> versionMap = getVersionMap();
        log.info("Committing entity {}, versions = {}", entity, versionMap);
        try {
            EntityPublishStatistics stats = entityMatchCommitter.commit(entity, tenant, null, versionMap);
            log.info("Entity {} committed. nSeeds={}, nLookups={}, nlookupNotInStaging={}", entity, stats.getSeedCount(),
                    stats.getLookupCount(), stats.getNotInStagingLookupCount());
            setStats(entity, stats.getSeedCount(), stats.getLookupCount());
            updateDataCollectionStatusVersion(versionMap);
        } catch(Exception e) {
            if (versionMap != null) {//Increase next version, avoid next PA reuse current next version number.
                int nextVersion = entityMatchVersionService.bumpNextVersion(SERVING, tenant);
                log.info("entity {} commit failed, the next version be changed to {}", entity, nextVersion);
            }
            throw e;
        }
    }

    private void setStats(String entity, int nSeeds, int nLookups) {
        // Assume CommitEntityMatch step might run multiple times but same
        // entity is only published once
        // entity name -> {"PUBLISH_SEED":nSeeds, "PUBLISH_LOOKUP":nLookups}
        Map<String, Map> entityPublishStats = getMapObjectFromContext(ENTITY_PUBLISH_STATS, String.class, Map.class);
        if (entityPublishStats == null) {
            entityPublishStats = new HashMap<>();
        }
        Map<String, Integer> cntMap = new HashMap<>();
        cntMap.put(ReportConstants.PUBLISH_SEED, nSeeds);
        cntMap.put(ReportConstants.PUBLISH_LOOKUP, nLookups);
        entityPublishStats.put(entity, cntMap);
        putObjectInContext(ENTITY_PUBLISH_STATS, entityPublishStats);
    }

    private Map<EntityMatchEnvironment, Integer> getVersionMap() {
        if (!Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class))) {
            log.info("Not in rematch mode, commit using current staging/serving versions");
            // not rematch, using current version
            return null;
        }
        Integer servingVersion = getObjectFromContext(ENTITY_MATCH_REMATCH_SERVING_VERSION, Integer.class);
        Integer stagingVersion = getObjectFromContext(ENTITY_MATCH_REMATCH_STAGING_VERSION, Integer.class);
        return ImmutableMap.of(STAGING, stagingVersion, SERVING, servingVersion);
    }

    private void updateDataCollectionStatusVersion(Map<EntityMatchEnvironment, Integer> versionMap) {
        if (versionMap == null || versionMap.get(SERVING) == null) {
            return;
        }
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        detail.setServingStoreVersion(versionMap.get(SERVING));
        putObjectInContext(CDL_COLLECTION_STATUS, detail);
        log.info("CommitEntityMatch step: dataCollection Status is " + JsonUtils.serialize(detail));
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(configuration.getCustomerSpace().toString(), detail, inactive);
    }

}
