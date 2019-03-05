package com.latticeengines.datacloud.workflow.match.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("commitEntityMatch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CommitEntityMatch extends BaseWorkflowStep<CommitEntityMatchConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CommitEntityMatch.class);

    private static final EntityMatchEnvironment SOURCE_ENV = EntityMatchEnvironment.STAGING;
    private static final EntityMatchEnvironment DEST_ENV = EntityMatchEnvironment.SERVING;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void execute() {
        log.info("In CommitEntityMatch.");
        Tenant tenant = tenantEntityMgr.findByTenantId(configuration.getCustomerSpace().toString());
        if (tenant == null) {
            throw new RuntimeException(
                    "Cannot find tenant with customerSpace: " + configuration.getCustomerSpace().toString());
        }
        Set<String> publishedEntities = getPublishedEntities();
        Set<String> commitEntities = getEntitySet(publishedEntities);
        commitEntities.forEach(entity -> publishEntity(tenant, entity));
        setPublishedEntities(commitEntities, publishedEntities);
    }

    @VisibleForTesting
    Set<String> getEntitySet(@NotNull Set<String> publishedEntities) {
        Set<String> entityImportSet = getEntityImportSet();
        Set<String> entitySet = new HashSet<>(entityImportSet);
        if (CollectionUtils.isNotEmpty(configuration.getEntitySet())) {
            entitySet.addAll(configuration.getEntitySet());
        }
        log.info("Entities to check for import = {}, checkAllEntitiesForImport = {}",
                configuration.getEntityImportSetToCheck(), configuration.isCheckAllEntityImport());
        log.info("Candidate entities to publish = {}. Entities with import = {}, entitySet in config = {}", entitySet,
                entityImportSet, configuration.getEntitySet());
        log.info("Already published entities = {}, skipPublishedEntities = {}", publishedEntities,
                configuration.isSkipPublishedEntities());
        return entitySet.stream() //
                // always publish if the flag to skip published entities is false
                // otherwise skip entities that are already published
                .filter(entity -> !configuration.isSkipPublishedEntities() || !publishedEntities.contains(entity)) //
                .collect(Collectors.toSet());
    }

    private Set<String> getPublishedEntities() {
        Set<String> entities = getSetObjectFromContext(PUBLISHED_ENTITIES, String.class);
        return entities == null ? Collections.emptySet() : entities;
    }

    /*
     * update published entity set (copy to a new set to be safe)
     */
    private void setPublishedEntities(Set<String> commitEntities, Set<String> publishedEntities) {
        Set<String> entities = new HashSet<>();
        entities.addAll(commitEntities);
        entities.addAll(publishedEntities);
        putObjectInContext(PUBLISHED_ENTITIES, entities);
    }

    /*
     * if configuration.isCheckAllEntityImport() flag is set, return all entities
     * that have import
     *
     * otherwise return all entities in configuration.getEntityImportSetToCheck()
     * AND have import
     */
    private Set<String> getEntityImportSet() {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (MapUtils.isNotEmpty(entityImportsMap)) {
            return entityImportsMap.keySet() //
                    .stream() //
                    .map(Enum::name) //
                    .filter(entity -> {
                        if (configuration.isCheckAllEntityImport()) {
                            return true;
                        }

                        Set<String> entitySet = configuration.getEntityImportSetToCheck();
                        return entitySet != null && entitySet.contains(entity);
                    }) //
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    private void publishEntity(Tenant tenant, String entity) {
        List<String> getSeedIds = new ArrayList<>();
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        int nSeeds = 0, nLookups = 0;
        do {
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(SOURCE_ENV, tenant, entity, getSeedIds,
                    1000);
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
                        if (seedIds.get(i).equals(seed.getId())) {
                            pairs.add(Pair.of(seed.getLookupEntries().get(i), seedIds.get(i)));
                        }
                    }

                }
                entityRawSeedService.batchCreate(DEST_ENV, tenant, scanSeeds, EntityMatchUtils.shouldSetTTL(DEST_ENV));
                entityLookupEntryService.set(DEST_ENV, tenant, pairs, EntityMatchUtils.shouldSetTTL(DEST_ENV));
                nSeeds += scanSeeds.size();
                nLookups += pairs.size();
            }
            scanSeeds.clear();
        } while (CollectionUtils.isNotEmpty(getSeedIds));
        log.info("Published {} seeds and {} lookup entries for entity = {}", nSeeds, nLookups, entity);
    }

}
