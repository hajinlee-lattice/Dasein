package com.latticeengines.remote.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

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
        assertEquals(segments.size(), 4);
    }

    @Test(groups = "functional", enabled = true)
    public void testParseSegmentSpec() throws Exception {
        DataLoaderServiceImpl dataLoaderServiceImpl = new DataLoaderServiceImpl();
        List<Segment> segments = dataLoaderServiceImpl.parseSegmentSpec(SEGMENTS_WITH_EMPTY_MODELIDS);
        assertEquals(segments.size(), 4);
        assertEquals(segments.get(0).getModelId(), "");
        assertEquals(segments.get(3).getModelId(), "defaultModelId");
    }

    @Test(groups = "functional", enabled = true)
    public void testSetSegments() throws Exception {
        List<Segment> segments = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            Segment segment = new Segment();
            segment.setName("ABC" + i);
            segment.setModelId("modelID" + i);
            segment.setPriority(i);
            segments.add(segment);
        }

        Segment defaultSegment = new Segment();
        defaultSegment.setName(DataLoaderServiceImpl.DEFAULT_SEGMENT);
        defaultSegment.setModelId("defaultModelId");
        defaultSegment.setPriority(7);
        segments.add(defaultSegment);
        InstallResult result = dataLoaderService.setSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL, segments);

        assertNull(result.getErrorMessage());
        assertEquals(result.getStatus(), 3);
    }

    @Test(groups = "functional", enabled = true, expectedExceptions = { LedpException.class } )
    public void testSetOutOfSyncSegments() throws Exception {
        List<Segment> segments = new ArrayList<>();

        Segment defaultSegment = new Segment();
        defaultSegment.setName(DataLoaderServiceImpl.DEFAULT_SEGMENT);
        defaultSegment.setModelId("defaultModelId");
        defaultSegment.setPriority(7);
        segments.add(defaultSegment);

        dataLoaderService.setSegments(SEGMENT_TEST_TENANT, TEST_DATALOADER_URL, segments);
    }
}
