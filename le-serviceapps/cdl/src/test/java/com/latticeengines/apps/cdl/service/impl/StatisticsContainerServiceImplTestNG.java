package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.StatisticsContainerService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
public class StatisticsContainerServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private StatisticsContainerService statisticsContainerService;

    @Inject
    private DataCollectionService dataCollectionService;

    private StatisticsContainer container;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
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

        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        container.setVersion(version);
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, container);
        container = dataCollectionService.getStats(mainCustomerSpace, collectionName, version);
        Long pid = container.getPid();
        StatisticsContainer container2 = statisticsContainerService.findByName(mainCustomerSpace, container.getName());
        Assert.assertEquals(container2.getPid(), pid);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testRetrieve() {
        StatisticsContainer retrieved = statisticsContainerService.findByName(mainCustomerSpace, container.getName());
        assertEquals(retrieved.getStatsCubes().size(), container.getStatsCubes().size());
        assertTrue(retrieved.getStatsCubes().containsKey("Account"));
        assertTrue(retrieved.getStatsCubes().containsKey("Contact"));

        StatsCube accountCube = retrieved.getStatsCubes().get("Account");
        AttributeStats attributeStats = accountCube.getStatistics().get("Foo");
        assertNotNull(attributeStats);
    }

    @Test(groups = "functional", dependsOnMethods = "testRetrieve")
    public void testDelete() {
        statisticsContainerService.delete(mainCustomerSpace, container.getName());
        StatisticsContainer retrieved = statisticsContainerService.findByName(mainCustomerSpace, container.getName());
        assertEquals(retrieved, null);
    }
}
