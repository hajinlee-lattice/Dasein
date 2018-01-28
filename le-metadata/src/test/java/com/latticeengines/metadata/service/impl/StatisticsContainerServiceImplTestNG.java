package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.StatisticsContainerService;

public class StatisticsContainerServiceImplTestNG extends DataCollectionFunctionalTestNGBase {

    @Autowired
    private StatisticsContainerService statisticsContainerService;

    @Autowired
    private DataCollectionService dataCollectionService;

    private StatisticsContainer container;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    @Test(groups = "functional")
    public void testCreate() {
        container = new StatisticsContainer();

        AttributeStats attributeStats = new AttributeStats();
        attributeStats.setNonNullCount(100L);
        StatsCube statsCube = new StatsCube();
        statsCube.setCount(200L);
        statsCube.setStatistics(ImmutableMap.of("Foo", attributeStats));
        container.setStatsCubes(ImmutableMap.of("Account", statsCube, "Contact", new StatsCube()));

        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        container.setVersion(version);
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, container);
        container = dataCollectionService.getStats(customerSpace1, collectionName, version);
        Long pid = container.getPid();
        StatisticsContainer container2 = statisticsContainerService.findByName(customerSpace1, container.getName());
        Assert.assertEquals(container2.getPid(), pid);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testRetrieve() {
        StatisticsContainer retrieved = statisticsContainerService.findByName(customerSpace1, container.getName());
        assertEquals(retrieved.getStatsCubes().size(), container.getStatsCubes().size());
        assertTrue(retrieved.getStatsCubes().containsKey("Account"));
        assertTrue(retrieved.getStatsCubes().containsKey("Contact"));

        StatsCube accountCube = retrieved.getStatsCubes().get("Account");
        AttributeStats attributeStats = accountCube.getStatistics().get("Foo");
        assertNotNull(attributeStats);
    }

    @Test(groups = "functional", dependsOnMethods = "testRetrieve")
    public void testDelete() {
        statisticsContainerService.delete(customerSpace1, container.getName());
        StatisticsContainer retrieved = statisticsContainerService.findByName(customerSpace1, container.getName());
        assertEquals(retrieved, null);
    }
}
