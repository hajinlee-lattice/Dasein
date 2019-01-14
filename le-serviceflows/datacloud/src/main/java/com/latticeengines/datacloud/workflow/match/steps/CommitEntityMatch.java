package com.latticeengines.datacloud.workflow.match.steps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;
import com.latticeengines.security.exposed.service.TenantService;
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
        Set<String> commitEntities = getEntitySet();
        if (CollectionUtils.isNotEmpty(commitEntities)) {
            commitEntities.forEach(entity -> publishEntity(tenant, entity));
        } else {
            log.error("There is no entity to publish");
        }
    }

    private Set<String> getEntitySet() {
        Set<String> entitySet = new HashSet<>();
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (MapUtils.isNotEmpty(entityImportsMap)) {
            entitySet.addAll(entityImportsMap.keySet().stream().map(Enum::name).collect(Collectors.toSet()));
            log.info("CONSOLIDATE_INPUT_IMPORTS entities set: " + entitySet.toString());
        }
        if (CollectionUtils.isNotEmpty(configuration.getEntitySet())) {
            entitySet.addAll(configuration.getEntitySet());
        }
        log.info("CommitEntityMatch entities set: " + entitySet.toString());
        return entitySet;
    }

    private void publishEntity(Tenant tenant, String entity) {
        List<String> getSeedIds = new ArrayList<>();
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
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
            }
            scanSeeds.clear();
        } while (CollectionUtils.isNotEmpty(getSeedIds));
    }

}
