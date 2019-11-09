package com.latticeengines.apps.cdl.service.impl;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class DimensionMetadataServiceImplTestNG extends CDLFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DimensionMetadataServiceImplTestNG.class);

    private static final String SIGNATURE = signature();
    private static final String STREAM_ID_1 = "s1";
    private static final String STREAM_ID_2 = "s2";
    private static final String STREAM_ID_3 = "s3";
    private static final List<String> STREAM_1_DIMS = Arrays.asList("d1", "d2");
    private static final List<String> STREAM_2_DIMS = Arrays.asList("d3", "d4", "d5");
    private static final List<String> STREAM_3_DIMS = singletonList("d6");
    private static final String VALUE = RandomStringUtils.randomAlphabetic(100);

    @Inject
    private DimensionMetadataServiceImpl dimensionMetadataService;

    private static String signature() {
        return DimensionMetadataServiceImplTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString();
    }

    @BeforeClass(groups = "functional")
    @AfterClass(groups = "functional")
    private void deleteSignature() {
        log.info("Deleting metadata for signature {}", SIGNATURE);
        dimensionMetadataService.delete(SIGNATURE);
    }

    @Test(groups = "functional")
    private void testPut() {
        for (String dimName : STREAM_1_DIMS) {
            dimensionMetadataService.put(SIGNATURE, STREAM_ID_1, dimName, defaultMetadata());
        }
    }

    @Test(groups = "functional")
    private void testPutAll() {
        Map<String, Map<String, DimensionMetadata>> metadataMap = new HashMap<>();
        metadataMap.put(STREAM_ID_2, new HashMap<>());
        metadataMap.put(STREAM_ID_3, new HashMap<>());
        STREAM_2_DIMS.forEach(dimName -> metadataMap.get(STREAM_ID_2).put(dimName, defaultMetadata()));
        STREAM_3_DIMS.forEach(dimName -> metadataMap.get(STREAM_ID_3).put(dimName, defaultMetadata()));
        dimensionMetadataService.put(SIGNATURE, metadataMap);
    }

    // read single dimension
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class, dataProvider = "readDimension", dependsOnMethods = {
            "testPut", "testPutAll" })
    private void testReadDimension(String streamId, String dimensionName, boolean shouldExist) {
        DimensionMetadata metadata = dimensionMetadataService.get(SIGNATURE, streamId, dimensionName);
        if (shouldExist) {
            Assert.assertEquals(metadata, defaultMetadata(),
                    String.format("Metadata for streamId %s and dimension %s should equal default metadata", streamId,
                            dimensionName));
        } else {
            Assert.assertNull(metadata, String.format("Metadata for streamId %s and dimension %s should not exist",
                    streamId, dimensionName));
        }
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class, dependsOnMethods = { "testPut",
            "testPutAll" })
    private void testReadStream() {
        Map<String, DimensionMetadata> stream2Metadata = dimensionMetadataService.getMetadataInStream(SIGNATURE,
                STREAM_ID_2);
        verifyStreamDimensions(stream2Metadata, new HashSet<>(STREAM_2_DIMS));

        // return empty map for non-existing stream
        Map<String, DimensionMetadata> fakeStreamDimensions = dimensionMetadataService.getMetadataInStream(SIGNATURE,
                "fake_stream_id");
        Assert.assertNotNull(fakeStreamDimensions);
        Assert.assertTrue(fakeStreamDimensions.isEmpty());
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class, dependsOnMethods = { "testPut",
            "testPutAll" })
    private void testReadSignature() {
        Map<String, Map<String, DimensionMetadata>> metadataMap = dimensionMetadataService.getMetadata(SIGNATURE);
        Assert.assertNotNull(metadataMap);
        Assert.assertEquals(metadataMap.size(), 3);
        Assert.assertEquals(metadataMap.keySet(), Sets.newHashSet(STREAM_ID_1, STREAM_ID_2, STREAM_ID_3));

        verifyStreamDimensions(metadataMap.get(STREAM_ID_1), new HashSet<>(STREAM_1_DIMS));
        verifyStreamDimensions(metadataMap.get(STREAM_ID_2), new HashSet<>(STREAM_2_DIMS));
        verifyStreamDimensions(metadataMap.get(STREAM_ID_3), new HashSet<>(STREAM_3_DIMS));
    }

    private void verifyStreamDimensions(Map<String, DimensionMetadata> dimensions, Set<String> expectedDimensionNames) {
        Assert.assertNotNull(dimensions);
        Assert.assertEquals(dimensions.size(), expectedDimensionNames.size());
        Assert.assertEquals(dimensions.keySet(), expectedDimensionNames);
        dimensions.values().forEach(metadata -> Assert.assertEquals(metadata, defaultMetadata()));
    }

    @DataProvider(name = "readDimension")
    private Object[][] readDimensionTestData() {
        return new Object[][] { //
                { STREAM_ID_1, STREAM_1_DIMS.get(0), true }, //
                { STREAM_ID_1, STREAM_1_DIMS.get(1), true }, //
                { STREAM_ID_1, STREAM_2_DIMS.get(0), false }, //
                { STREAM_ID_1, "sldkfj", false }, //
                { STREAM_ID_2, STREAM_1_DIMS.get(0), false }, //
                { STREAM_ID_2, STREAM_2_DIMS.get(2), true }, //
                { STREAM_ID_3, STREAM_3_DIMS.get(0), true }, //
                { "fake_stream_id", STREAM_3_DIMS.get(0), false }, //
        }; //
    }

    private DimensionMetadata defaultMetadata() {
        DimensionMetadata metadata = new DimensionMetadata();
        metadata.setCardinality(100L);
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            values.add(ImmutableMap.of("key", VALUE));
        }
        metadata.setDimensionValues(values);
        return metadata;
    }
}
