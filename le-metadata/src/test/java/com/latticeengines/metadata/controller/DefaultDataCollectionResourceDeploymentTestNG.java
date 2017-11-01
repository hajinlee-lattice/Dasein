package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;

public class DefaultDataCollectionResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DefaultDataCollectionResourceDeploymentTestNG.class);

    private static final String METADATA_SEGMENT_NAME = "MetadataSegment_Name";
    private static final String METADATA_SEGMENT_DISPLAY_NAME = "MetadataSegment_DisplayName";

    @Autowired
    protected DataCollectionProxy dataCollectionProxy;

    @Autowired
    protected SegmentProxy segmentProxy;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
    }

    @Test(groups = "deployment")
    public void testSwitchVersion() throws IOException {
        log.info("Switch version for " + customerSpace1);
        dataCollectionProxy.switchVersion(customerSpace1, DataCollection.Version.Green);

        DataCollection dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace1);
        assertNotNull(dataCollection);
        assertEquals(dataCollection.getVersion(), DataCollection.Version.Green);
    }

    @Test(groups = "deployment")
    public void testGetTable() throws IOException {
        log.info("Upsert table for " + customerSpace1);
        dataCollectionProxy.upsertTable(customerSpace1, TABLE1, TableRoleInCollection.Profile, DataCollection.Version
                .Blue);

        log.info("Get table without version");
        Table table = dataCollectionProxy.getTable(customerSpace1, TableRoleInCollection.Profile);

        assertNotNull(table);
        assertEquals(table.getName(), TABLE1);

        log.info("Get table with version");
        Table table1 = dataCollectionProxy.getTable(customerSpace1, TableRoleInCollection.Profile, DataCollection.Version.Blue);
        assertNotNull(table);
        assertEquals(table.getName(), table1.getName());

        Table nullTable = dataCollectionProxy.getTable(customerSpace1, TableRoleInCollection.AccountMaster);
        assertNull(nullTable);
    }

    @Test(groups = "deployment")
    public void testGetSegments() throws IOException {
        log.info("Create or update segment for " + customerSpace1);
        MetadataSegment metadataSegment = new MetadataSegment();
        metadataSegment.setName(METADATA_SEGMENT_NAME);
        metadataSegment.setDisplayName(METADATA_SEGMENT_DISPLAY_NAME);
        metadataSegment.setTenant(tenant1);
        segmentProxy.createOrUpdateSegment(customerSpace1, metadataSegment);

        log.info("Get segment");
        List<MetadataSegment> metadataSegments = dataCollectionProxy.getSegments(customerSpace1);
        assertNotNull(metadataSegments);
        assertEquals(metadataSegments.size(), 1);
        assertEquals(metadataSegments.get(0).getName(), METADATA_SEGMENT_NAME);
    }

    @Test(groups = "deployment")
    public void testGetMainStats() throws IOException {
        StatisticsContainer statisticsContainer = new StatisticsContainer();

        Statistics statistics = new Statistics();
        statistics.getCategories().put(Category.ACCOUNT_INFORMATION, new CategoryStatistics());
        statisticsContainer.setStatistics(statistics);

        DataCollection.Version version = dataCollectionProxy.getDefaultDataCollection(customerSpace1).getVersion();
        statisticsContainer.setVersion(version);

        log.info("Upsert stats for " + customerSpace1);
        dataCollectionProxy.upsertStats(customerSpace1, statisticsContainer);

        StatisticsContainer sc = dataCollectionProxy.getStats(customerSpace1, version);

        assertNotNull(sc);
        assertEquals(sc.getVersion(), version);
    }

    @Test(groups = "deployment")
    public void testGetAttrRepo() throws IOException {
        log.info("Get attribute repository");
        AttributeRepository attributeRepository = dataCollectionProxy.getAttrRepo(customerSpace1);
        assertNull(attributeRepository);
    }

    @Test(groups = "deployment")
    public void testResetTable() throws IOException {
        log.info("Reset table for " + customerSpace1);
        dataCollectionProxy.resetTable(customerSpace1, TableRoleInCollection.Profile);
        Table table = dataCollectionProxy.getTable(customerSpace1, TableRoleInCollection.Profile);

        assertNull(table);
    }
}
