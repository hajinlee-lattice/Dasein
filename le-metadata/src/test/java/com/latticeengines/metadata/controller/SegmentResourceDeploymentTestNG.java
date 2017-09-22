package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;

public class SegmentResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(SegmentResourceDeploymentTestNG.class);

    private static final String METADATA_SEGMENT_NAME = "MetadataSegment_Name";
    private static final String METADATA_SEGMENT_DISPLAY_NAME = "MetadataSegment_DisplayName";

    protected MetadataSegment metadataSegment;

    @Autowired
    protected SegmentProxy segmentProxy;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();

        metadataSegment = new MetadataSegment();
        metadataSegment.setName(METADATA_SEGMENT_NAME);
        metadataSegment.setDisplayName(METADATA_SEGMENT_DISPLAY_NAME);
        metadataSegment.setTenant(tenant1);
    }

    @Test(groups = "deployment")
    public void testCreateOrUpdateSegment() throws IOException {
        metadataSegment = segmentProxy.createOrUpdateSegment(customerSpace1, metadataSegment);

        MetadataSegment msTest = segmentProxy.getMetadataSegmentByName(customerSpace1,
                METADATA_SEGMENT_NAME);

        assertNotNull(msTest);
        assertEquals(msTest.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);

        metadataSegment.setDescription("MetadataSegment_Description");
        metadataSegment = segmentProxy.createOrUpdateSegment(customerSpace1, metadataSegment);

        msTest = segmentProxy.getMetadataSegmentByName(customerSpace1,
                METADATA_SEGMENT_NAME);

        assertNotNull(msTest);
        assertEquals(msTest.getDescription(), "MetadataSegment_Description");
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateOrUpdateSegment")
    public void testGetSegments() throws IOException {
        List<MetadataSegment> metadataSegments = segmentProxy.getMetadataSegments(customerSpace1);

        assertNotNull(metadataSegments);
        assertEquals(metadataSegments.size(), 1);
        assertEquals(metadataSegments.get(0).getName(), METADATA_SEGMENT_NAME);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetSegments")
    public void testGetSegmentWithPid() throws IOException {
        MetadataSegmentDTO metadataSegmentDTO = segmentProxy.getMetadataSegmentWithPidByName(customerSpace1,
                METADATA_SEGMENT_NAME);

        assertNotNull(metadataSegmentDTO);
        MetadataSegment msTest = metadataSegmentDTO.getMetadataSegment();
        assertNotNull(msTest);
        assertEquals(msTest.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetSegmentWithPid")
    public void testUpsertStatsToSegment() throws IOException {
        StatisticsContainer statisticsContainer = new StatisticsContainer();
        statisticsContainer.setVersion(DataCollection.Version.Blue);
        statisticsContainer.setStatistics(new Statistics());

        SimpleBooleanResponse response = segmentProxy.upsertStatsToSegment(customerSpace1, METADATA_SEGMENT_NAME,
                statisticsContainer);
        assertTrue(response.isSuccess());

        StatisticsContainer sc = segmentProxy.getSegmentStats(customerSpace1, METADATA_SEGMENT_NAME, null);
        assertNotNull(sc);
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpsertStatsToSegment")
    public void testDeleteSegmentByName() throws IOException {
        segmentProxy.deleteSegmentByName(customerSpace1, METADATA_SEGMENT_NAME);

        MetadataSegment metadataSegmentTest = segmentProxy.getMetadataSegmentByName(customerSpace1,
                METADATA_SEGMENT_NAME);

        assertNull(metadataSegmentTest);
    }
}