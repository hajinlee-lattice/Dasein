package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.JourneyStageService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStagePredicates;
import com.latticeengines.domain.exposed.cdl.activity.StreamFieldToFilter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class JourneyStageServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(JourneyStageServiceImplTestNG.class);

    @Inject
    private JourneyStageService journeyStageService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCRUD() {
        String stageName = "journeyStage1";
        int priority = 3;
        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setColumnName(InterfaceName.StageName.name());
        filter.setColumnValue("Close%");
        filter.setCompareType(StreamFieldToFilter.CompareType.Contains);
        JourneyStagePredicates predicates = new JourneyStagePredicates();
        predicates.setContactNotNull(false);
        predicates.setNoOfEvents(3);
        predicates.setPeriod(30);
        predicates.setStreamType(AtlasStream.StreamType.WebVisit);
        predicates.setStreamFieldToFilterList(Collections.singletonList(filter));
        JourneyStage journeyStage = new JourneyStage();
        journeyStage.setPredicates(Collections.singletonList(predicates));
        journeyStage.setStageName(stageName);
        journeyStage.setPriority(priority);
        journeyStage.setTenant(mainTestTenant);
        JourneyStage created = journeyStageService.createOrUpdate(mainCustomerSpace, journeyStage);
        log.info("JourneyStage is {}.", JsonUtils.serialize(created));
        log.info("pid is {}", created.getPid());
        Assert.assertNotNull(created.getPid());
        List<JourneyStage> journeyStageList = journeyStageService.findByTenant(mainCustomerSpace);
        Assert.assertEquals(journeyStageList.size(), 1);
        created = journeyStageList.get(0);
        log.info("JourneyStage is {}.", JsonUtils.serialize(created));
        Assert.assertEquals(created.getStageName(), stageName);
        Assert.assertEquals(created.getPriority(), priority);
        List<JourneyStagePredicates> predicateList = created.getPredicates();
        Assert.assertEquals(predicateList.size(), 1);
        JourneyStagePredicates createdPre = predicateList.get(0);
        Assert.assertEquals(createdPre.getNoOfEvents(), predicates.getNoOfEvents());
        Assert.assertEquals(createdPre.getPeriod(), predicates.getPeriod());
        Assert.assertEquals(createdPre.getStreamType(), predicates.getStreamType());
        List<StreamFieldToFilter> createdFilter = createdPre.getStreamFieldToFilterList();
        Assert.assertEquals(createdFilter.size(), 1);
        Assert.assertEquals(createdFilter.get(0).getColumnName(), filter.getColumnName());
        Assert.assertEquals(createdFilter.get(0).getColumnValue(), filter.getColumnValue());
        Assert.assertEquals(createdFilter.get(0).getCompareType(), filter.getCompareType());
    }
}
