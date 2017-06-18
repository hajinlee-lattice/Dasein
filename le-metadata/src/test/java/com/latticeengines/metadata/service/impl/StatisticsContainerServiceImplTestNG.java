package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
        Statistics statistics = new Statistics();
        statistics.getCategories().put(Category.ACCOUNT_INFORMATION, new CategoryStatistics());
        SubcategoryStatistics subcategoryStatistics = new SubcategoryStatistics();
        subcategoryStatistics.getAttributes().put(new AttributeLookup(BusinessEntity.Account, "Foo"),
                new AttributeStats());
        statistics.getCategories().get(Category.ACCOUNT_INFORMATION).getSubcategories()
                .put(ColumnMetadata.SUBCATEGORY_OTHER, subcategoryStatistics);
        container.setStatistics(statistics);
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, container, null);
        container = dataCollectionService.getStats(customerSpace1, collectionName, null);
        Long pid = container.getPid();
        StatisticsContainer container2 = statisticsContainerService.findByName(customerSpace1, container.getName());
        Assert.assertEquals(container2.getPid(), pid);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testRetrieve() {
        StatisticsContainer retrieved = statisticsContainerService.findByName(customerSpace1, container.getName());
        assertEquals(retrieved.getStatistics().getCategories().size(), container.getStatistics().getCategories().size());
        assertTrue(retrieved.getStatistics().getCategories().containsKey(Category.ACCOUNT_INFORMATION));
        CategoryStatistics categoryStatistics = retrieved.getStatistics().getCategories()
                .get(Category.ACCOUNT_INFORMATION);
        SubcategoryStatistics subcategoryStatistics = categoryStatistics.getSubcategories().get(
                ColumnMetadata.SUBCATEGORY_OTHER);
        AttributeStats attributeStatistics = subcategoryStatistics.getAttributes().get(
                new AttributeLookup(BusinessEntity.Account, "Foo"));
        assertNotNull(attributeStatistics);
    }

    @Test(groups = "functional", dependsOnMethods = "testRetrieve")
    public void testDelete() {
        statisticsContainerService.delete(customerSpace1, container.getName());
        StatisticsContainer retrieved = statisticsContainerService.findByName(customerSpace1, container.getName());
        assertEquals(retrieved, null);
    }
}
