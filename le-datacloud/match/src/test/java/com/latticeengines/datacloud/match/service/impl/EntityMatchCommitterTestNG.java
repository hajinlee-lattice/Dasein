package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchCommitter;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class EntityMatchCommitterTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(EntityMatchCommitterTestNG.class);

    @Inject
    private EntityMatchCommitter entityMatchCommitter;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Test(groups = "functional", dataProvider = "commit")
    private void testCommit(Integer nSeeds) {
        Tenant tenant = newTestTenant();
        String entity = BusinessEntity.Account.name();

        log.info("Preparing test data of {} seeds, entity={}, tenant={}", nSeeds, entity, tenant.getId());
        Pair<Integer, Integer> pair = prepareTestData(tenant, entity, nSeeds);
        log.info("Prepared {} seeds and {} lookup entries", pair.getLeft(), pair.getRight());

        EntityPublishStatistics res = entityMatchCommitter.commit(entity, tenant, true);
        Assert.assertNotNull(res);
        // make sure published counts are as expected
        Assert.assertEquals(res.getSeedCount(), (int) pair.getKey(), "Number of seeds should match");
        Assert.assertEquals(res.getLookupCount(), (int) pair.getValue(), "Number of lookup entries should match");
        Assert.assertEquals(res.getNotInStagingLookupCount(), 0);
    }

    @DataProvider(name = "commit")
    private Object[][] commitTestData() {
        return new Object[][] { //
                { 1 }, // small number of records to make sure no race condition
                { 500 }, //
        }; //
    }

    private Tenant newTestTenant() {
        return new Tenant(getClass().getSimpleName() + "_" + UUID.randomUUID().toString());
    }

    private Pair<Integer, Integer> prepareTestData(Tenant tenant, String entity, int nSeeds) {
        List<EntityRawSeed> seeds = new ArrayList<>();
        List<Pair<EntityLookupEntry, String>> entries = new ArrayList<>();
        for (int i = 0; i < nSeeds; i++) {
            // fake three lookup entries
            EntityLookupEntry dom = EntityLookupEntryConverter.fromDomainCountry(entity, "Domain" + i, "USA");
            EntityLookupEntry name = EntityLookupEntryConverter.fromNameCountry(entity, "Company" + i, "USA");
            EntityLookupEntry id = EntityLookupEntryConverter.fromSystemId(entity, "id", "id" + i);
            String entityId = String.valueOf(i);
            EntityRawSeed seed = new EntityRawSeed(entityId, entity, Arrays.asList(dom, name, id), null);
            seeds.add(seed);
            entries.add(Pair.of(dom, entityId));
            entries.add(Pair.of(name, entityId));
            entries.add(Pair.of(id, entityId));
        }

        entityRawSeedService.batchCreate(EntityMatchEnvironment.STAGING, tenant, seeds, true);
        entityLookupEntryService.set(EntityMatchEnvironment.STAGING, tenant, entries, true);
        return Pair.of(seeds.size(), entries.size());
    }
}
