package com.latticeengines.datacloud.workflow.match.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchCommitter;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * This step only runs once before switch stack.
 * It attempts to commit all supported entities, if STAGING version is bumped up.
 */
@Component("commitEntityMatch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CommitEntityMatch extends BaseWorkflowStep<CommitEntityMatchConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CommitEntityMatch.class);

    private static final EntityMatchEnvironment SOURCE_ENV = EntityMatchEnvironment.STAGING;
    private static final EntityMatchEnvironment DEST_ENV = EntityMatchEnvironment.SERVING;

    // only commit Account & Contact for now
    private static final Set<String> ENTITIES_TO_COMMIT = Sets.newHashSet( //
            BusinessEntity.Account.name(), BusinessEntity.Contact.name());

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    @Lazy
    private EntityMatchCommitter entityMatchCommitter;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${cdl.processAnalyze.entity.commit.parallel}")
    private boolean useParallelCommitter;

    @Override
    public void execute() {
        List<EntityMatchEnvironment> updatedEnvs = //
                getListObjectFromContext(NEW_ENTITY_MATCH_ENVS, EntityMatchEnvironment.class);
        if (CollectionUtils.isNotEmpty(updatedEnvs) && updatedEnvs.contains(EntityMatchEnvironment.STAGING)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(configuration.getCustomerSpace().toString());
            if (tenant == null) {
                throw new RuntimeException(
                        "Cannot find tenant with customerSpace: " + configuration.getCustomerSpace().toString());
            }

            log.info("Use {} committer to commit entities", useParallelCommitter ? "parallel" : "sequential");
            Tenant standardizedTenant = EntityMatchUtils.newStandardizedTenant(tenant);
            ENTITIES_TO_COMMIT.forEach(entity -> {
                if (useParallelCommitter) {
                    commitWithCommitter(standardizedTenant, entity);
                } else {
                    commitEntity(standardizedTenant, entity);
                }
            });
        }
    }

    private void commitWithCommitter(Tenant tenant, String entity) {
        log.info("Committing entity {}", entity);
        EntityPublishStatistics stats = entityMatchCommitter.commit(entity, tenant, null);
        log.info("Entity {} committed. nSeeds={}, nLookups={}, nlookupNotInStaging={}", entity, stats.getSeedCount(),
                stats.getLookupCount(), stats.getNotInStagingLookupCount());
        setStats(entity, stats.getSeedCount(), stats.getLookupCount());
    }

    private void commitEntity(Tenant tenant, String entity) {
        List<String> getSeedIds = new ArrayList<>();
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        int nSeeds = 0, nLookups = 0;
        int nNotInStaging = 0;
        do {
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(SOURCE_ENV, tenant, entity, getSeedIds,
                    1000, null);
            getSeedIds.clear();
            if (MapUtils.isNotEmpty(seeds)) {
                for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                    getSeedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                    scanSeeds.addAll(entry.getValue());
                }
                List<Pair<EntityLookupEntry, String>> pairs = new ArrayList<>();
                for (EntityRawSeed seed : scanSeeds) {
                    List<String> seedIds = entityLookupEntryService.get(SOURCE_ENV, tenant, seed.getLookupEntries());
                    for(int i = 0; i < seedIds.size(); i++) {
                        if (seedIds.get(i) == null) {
                            nNotInStaging++;
                            continue;
                        }
                        if (seedIds.get(i).equals(seed.getId())) {
                            pairs.add(Pair.of(seed.getLookupEntries().get(i), seedIds.get(i)));
                        }
                    }

                }
                entityRawSeedService.batchCreate(DEST_ENV, tenant, scanSeeds, EntityMatchUtils.shouldSetTTL(DEST_ENV),
                        null);
                entityLookupEntryService.set(DEST_ENV, tenant, pairs, EntityMatchUtils.shouldSetTTL(DEST_ENV));
                nSeeds += scanSeeds.size();
                nLookups += pairs.size();
            }
            scanSeeds.clear();
        } while (CollectionUtils.isNotEmpty(getSeedIds));
        log.info("Published {} seeds and {} lookup entries for entity = {}. {} lookup entries are not in staging.",
                nSeeds, nLookups, entity, nNotInStaging);
        setStats(entity, nSeeds, nLookups);
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

}
