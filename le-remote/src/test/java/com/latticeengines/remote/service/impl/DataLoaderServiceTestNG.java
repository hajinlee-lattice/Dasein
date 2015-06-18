package com.latticeengines.remote.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class DataLoaderServiceTestNG extends RemoteFunctionalTestNGBase {

    public static final String SEGMENT_TEST_TENANT = "MW_Devel_SegmentTesting_20150511";
    private static final String TEST_DATALOADER_URL = "http://10.41.1.187:8081/DLRestService";
    private static final String SEGMENTS_WITH_EMPTY_MODELIDS = "SpecLatticeFunction(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier(\"IF\"), LatticeFunctionIdentifier(ContainerElementName(\"DefnSegment_ABC1\")), LatticeFunctionExpressionConstant(\"\", DataTypeVarChar(0)), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier(\"IF\"), LatticeFunctionIdentifier(ContainerElementName(\"DefnSegment_ABC2\")), LatticeFunctionExpressionConstant(\"\", DataTypeVarChar(0)), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier(\"IF\"), LatticeFunctionIdentifier(ContainerElementName(\"DefnSegment_ABC3\")), LatticeFunctionExpressionConstant(\"ms__8195dcf1-0898-4ad3-b94d-0d0f806e979e-PLSModel-Eloqua\", DataTypeVarChar(56)), LatticeFunctionExpressionConstant(\"defaultModelId\", DataTypeVarChar(14))))), DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription(\"\"))";

    @Resource(name = "dataLoaderService")
    private DataLoaderService dataLoaderService;

    @BeforeClass(groups = "functional")
    public void setup() {
    }

    @Test(groups = "functional", enabled = true)
    public void testGetSegments() throws Exception {
        List<Segment> segments = dataLoaderService.getSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL);
        assertEquals(segments.size(), 3);

        List<String> segmentNames = dataLoaderService.getSegmentNames(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL);
        assertEquals(segmentNames.size(), 2);
    }

    @Test(groups = "functional", enabled = true)
    public void testParseSegmentSpec() throws Exception {
        DataLoaderServiceImpl dataLoaderServiceImpl = new DataLoaderServiceImpl();
        Map<String, Segment> segments = dataLoaderServiceImpl.parseSegmentSpec(SEGMENTS_WITH_EMPTY_MODELIDS);
        assertEquals(segments.size(), 4);
        assertEquals(segments.get("ABC1").getModelId(), "");
        assertEquals(segments.get(DataLoaderServiceImpl.DEFAULT_SEGMENT).getModelId(), "defaultModelId");
    }

    @Test(groups = "functional", enabled = true)
    public void testSetSegments() throws Exception {
        List<Segment> segments = new ArrayList<>();

        Segment segmentUS = new Segment();
        segmentUS.setName("US");
        segmentUS.setModelId("US_modelID");
        segmentUS.setPriority(2);
        segments.add(segmentUS);

        Segment segmentSpain = new Segment();
        segmentSpain.setName("Spain");
        segmentSpain.setModelId("Spain_modelID");
        segmentSpain.setPriority(1);
        segments.add(segmentSpain);

        Segment defaultSegment = new Segment();
        defaultSegment.setName(DataLoaderServiceImpl.DEFAULT_SEGMENT);
        defaultSegment.setModelId("defaultModelId");
        defaultSegment.setPriority(7);
        segments.add(defaultSegment);
        InstallResult result = dataLoaderService.setSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL, segments);

        assertNull(result.getErrorMessage());
        assertEquals(result.getStatus(), 3);

        List<Segment> retrievedSegments = dataLoaderService.getSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL);
        assertEquals(retrievedSegments.get(0).getName(), "Spain");
        assertEquals(retrievedSegments.get(1).getName(), "US");
        assertEquals(retrievedSegments.get(2).getName(), DataLoaderServiceImpl.DEFAULT_SEGMENT);

        segmentUS.setPriority(1);
        segmentSpain.setPriority(2);
        result = dataLoaderService.setSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL, segments);
        assertNull(result.getErrorMessage());
        assertEquals(result.getStatus(), 3);

        retrievedSegments = dataLoaderService.getSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL);
        assertEquals(retrievedSegments.get(0).getName(), "US");
        assertEquals(retrievedSegments.get(1).getName(), "Spain");
        assertEquals(retrievedSegments.get(2).getName(), DataLoaderServiceImpl.DEFAULT_SEGMENT);
    }

    @Test(groups = "functional", enabled = true, expectedExceptions = { LedpException.class })
    public void testSetOutOfSyncSegments() throws Exception {
        List<Segment> segments = new ArrayList<>();

        Segment defaultSegment = new Segment();
        defaultSegment.setName(DataLoaderServiceImpl.DEFAULT_SEGMENT);
        defaultSegment.setModelId("defaultModelId");
        defaultSegment.setPriority(7);
        segments.add(defaultSegment);

        dataLoaderService.setSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL, segments);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetTemplateVersion() throws Exception {

        String version = dataLoaderService.getTemplateVersion(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL);
        Assert.assertEquals(version, "");
    }
}
