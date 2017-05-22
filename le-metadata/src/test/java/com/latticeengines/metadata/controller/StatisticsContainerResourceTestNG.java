package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.StatisticsContainerProxy;

public class StatisticsContainerResourceTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private StatisticsContainerProxy statisticsContainerProxy;

    private StatisticsContainer container;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void testCreate() {
        container = new StatisticsContainer();
        Statistics statistics = new Statistics();
        statistics.getCategories().put(Category.ACCOUNT_INFORMATION.getName(), new CategoryStatistics());
        container.setStatistics(statistics);
        container = statisticsContainerProxy.createOrUpdateStatistics(customerSpace1, container);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testRetrieve() {
        StatisticsContainer retrieved = statisticsContainerProxy.getStatistics(customerSpace1, container.getName());
        assertEquals(retrieved.getStatistics().getCategories().size(), container.getStatistics().getCategories().size());
        assertTrue(retrieved.getStatistics().getCategories().containsKey(Category.ACCOUNT_INFORMATION.getName()));
    }

    @Test(groups = "functional", dependsOnMethods = "testRetrieve")
    public void testDelete() {
        statisticsContainerProxy.deleteStatistics(customerSpace1, container.getName());
        StatisticsContainer retrieved = statisticsContainerProxy.getStatistics(customerSpace1, container.getName());
        assertEquals(retrieved, null);
    }
}
