package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.JourneyStageEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStagePredicate;
import com.latticeengines.domain.exposed.cdl.activity.StreamFieldToFilter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;

public class JourneyStageEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(JourneyStageEntityMgrImplTestNG.class);

    @Inject
    private JourneyStageEntityMgr journeyStageEntityMgr;
    private String stageName = "journeyStage1";
    private String stageDescription = "Some Description";
    private String stageDisplayColorCode = "#ColorCode";
    private int priority = 3;
    private StreamFieldToFilter filter;
    private JourneyStagePredicate predicates;
    private JourneyStage journeyStage;
    private String updateStageName = "journeyStage2";
    private String updatedDescription = "Updated Description";
    private Long pid;
    private RetryTemplate retry;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testCreate() {
        filter = new StreamFieldToFilter();
        filter.setColumnName(InterfaceName.StageName);
        filter.setColumnValue("Close%");
        filter.setComparisonType(ComparisonType.CONTAINS);
        predicates = new JourneyStagePredicate();
        predicates.setContactNotNull(false);
        predicates.setNoOfEvents(3);
        predicates.setPeriodDays(30);
        predicates.setStreamType(AtlasStream.StreamType.WebVisit);
        predicates.setStreamFieldsToFilter(Collections.singletonList(filter));
        journeyStage = new JourneyStage();
        journeyStage.setPredicates(Collections.singletonList(predicates));
        journeyStage.setStageName(stageName);
        journeyStage.setDisplayName(stageName);
        journeyStage.setDisplayColorCode(stageDisplayColorCode);
        journeyStage.setDescription(stageDescription);
        journeyStage.setPriority(priority);
        journeyStage.setTenant(mainTestTenant);
        journeyStageEntityMgr.create(journeyStage);
        List<JourneyStage> journeyStageList = journeyStageEntityMgr.findByTenant(mainTestTenant);
        Assert.assertEquals(journeyStageList.size(), 1);
        JourneyStage created = journeyStageList.get(0);
        log.info("JourneyStage is {}.", JsonUtils.serialize(created));
        pid = created.getPid();
        Assert.assertEquals(created.getStageName(), stageName);
        Assert.assertEquals(created.getPriority(), priority);
        Assert.assertNotNull(created.getCreated());
        Assert.assertNotNull(created.getUpdated());
        List<JourneyStagePredicate> predicateList = created.getPredicates();
        Assert.assertEquals(predicateList.size(), 1);
        JourneyStagePredicate createdPre = predicateList.get(0);
        Assert.assertEquals(createdPre.getNoOfEvents(), predicates.getNoOfEvents());
        Assert.assertEquals(createdPre.getPeriodDays(), predicates.getPeriodDays());
        Assert.assertEquals(createdPre.getStreamType(), predicates.getStreamType());

        List<StreamFieldToFilter> createdFilter = createdPre.getStreamFieldsToFilter();
        Assert.assertEquals(createdFilter.size(), 1);
        Assert.assertEquals(createdFilter.get(0).getColumnName(), filter.getColumnName());
        Assert.assertEquals(createdFilter.get(0).getColumnValue(), filter.getColumnValue());
        Assert.assertEquals(createdFilter.get(0).getComparisonType(), filter.getComparisonType());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testUpdate() {
        AtomicReference<JourneyStage> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(journeyStageEntityMgr.findByPid(pid));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        JourneyStage stage = createdAtom.get();
        Assert.assertNotNull(stage);
        Assert.assertEquals(stage.getStageName(), stageName);
        stage.setStageName(updateStageName);
        stage.setDescription(updatedDescription);
        journeyStageEntityMgr.createOrUpdate(stage);
        retry.execute(context -> {
            createdAtom.set(journeyStageEntityMgr.findByTenantAndStageName(mainTestTenant, updateStageName));
            Assert.assertNotNull(createdAtom.get());
            Assert.assertEquals(createdAtom.get().getDescription(), updatedDescription);
            return true;
        });
        stage = createdAtom.get();
        Assert.assertNotNull(stage);
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdate")
    public void testDelete() {
        AtomicReference<JourneyStage> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(journeyStageEntityMgr.findByPid(pid));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        JourneyStage stage = createdAtom.get();
        Assert.assertNotNull(stage);
        Assert.assertEquals(stage.getStageName(), updateStageName);
        stage.setTenant(null);
        journeyStageEntityMgr.delete(stage);
        retry.execute(context -> {
            createdAtom.set(journeyStageEntityMgr.findByTenantAndStageName(mainTestTenant, updateStageName));
            Assert.assertNull(createdAtom.get());
            return true;
        });
        stage = createdAtom.get();
        Assert.assertNull(stage);
    }
}
