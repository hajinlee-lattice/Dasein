package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class StatsCubeUtilsUnitTestNG {

    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed/util/statsCubeUtilsUnitTestNG/";

    @Test(groups = "unit")
    public void testParseAvro() throws Exception {
        Iterator<GenericRecord> records = readAvro();
        StatsCube cube = StatsCubeUtils.parseAvro(records);
        Assert.assertNotNull(cube);
        AttributeStats stats = cube.getStatistics().get("LatticeAccountId");
        long maxCount = stats.getNonNullCount();
        Assert.assertEquals(cube.getCount(), new Long(maxCount));

        cube.getStatistics().forEach((attrName, attrStats) -> //
        Assert.assertTrue(attrStats.getNonNullCount() <= maxCount, attrName + JsonUtils.pprint(attrStats)));
    }

    @Test(groups = "unit")
    public void testConstructStatsContainer() throws Exception {
        ObjectMapper om = new ObjectMapper();
        Map<BusinessEntity, List<ColumnMetadata>> cmMap = om.readValue(readResource("mdMap.json"),
                new TypeReference<Map<BusinessEntity, List<ColumnMetadata>>>() {
                });
        Map<BusinessEntity, StatsCube> cubeMap = om.readValue(readResource("cubeMap.json"),
                new TypeReference<Map<BusinessEntity, StatsCube>>() {
                });
        Statistics statistics = StatsCubeUtils.constructStatistics(cubeMap, cmMap);
        Assert.assertNotNull(statistics);
    }

    @Test(groups = "unit")
    public void testTopN() throws Exception {
        InputStream is = readResource("statistics.json.gz");
        GZIPInputStream gis = new GZIPInputStream(is);
        Statistics statistics = JsonUtils.deserialize(gis, StatisticsContainer.class).getStatistics();
        TopNTree topNTree = StatsCubeUtils.toTopNTree(statistics, true);
        Assert.assertTrue(MapUtils.isNotEmpty(topNTree.getCategories()));
        StatsCube cube = StatsCubeUtils.toStatsCube(statistics);

        verifyFirmographicsTopN(topNTree.getCategories().get(Category.FIRMOGRAPHICS), cube);
        verifyIntentTopN(topNTree.getCategories().get(Category.INTENT), cube);
        verifyTechTopN(topNTree.getCategories().get(Category.WEBSITE_PROFILE), cube);
        verifyTechTopN(topNTree.getCategories().get(Category.TECHNOLOGY_PROFILE), cube);
        verifyPurchaseHistoryTopN(topNTree.getCategories().get(Category.PRODUCT_SPEND));
    }

    private void verifyFirmographicsTopN(CategoryTopNTree catTopNTree, StatsCube cube) {
        List<TopAttribute> topAttrs = catTopNTree.getSubcategories().get("Other");
        Assert.assertTrue(topAttrs.size() >= 5, "Should have at least 5 attributes in Firmographics");
        int idx = 0;
        for (String expectedAttr : Arrays.asList(DataCloudConstants.ATTR_LDC_INDUSTRY, //
                DataCloudConstants.ATTR_NUM_EMP_RANGE, //
                DataCloudConstants.ATTR_REV_RANGE, //
                DataCloudConstants.ATTR_COUNTRY, //
                DataCloudConstants.ATTR_IS_PRIMARY_LOCATION)) {
            TopAttribute attr = topAttrs.get(idx++);
            Assert.assertEquals(attr.getAttribute(), expectedAttr);
            verifyTopBkt(cube, attr.getAttribute(), attr.getTopBkt(), //
                    StatsCubeUtils.getBktComparatorForCategory(Category.FIRMOGRAPHICS));
        }
    }

    private void verifyIntentTopN(CategoryTopNTree catTopNTree, StatsCube cube) {
        catTopNTree.getSubcategories().values().forEach(attrs -> {
            Long previousCount = null;
            for (TopAttribute attr : attrs) {
                Bucket topBkt = attr.getTopBkt();
                if (topBkt != null) {
                    Long currentCount = topBkt.getCount();
                    if (previousCount != null) {
                        Assert.assertTrue(currentCount <= previousCount, String.format(
                                "Current count %d is bigger than previous count %d", currentCount, previousCount));
                    }
                    previousCount = currentCount;

                    AttributeStats attributeStats = cube.getStatistics().get(attr.getAttribute());
                    Buckets buckets = attributeStats.getBuckets();
                    if (buckets != null) {
                        List<Bucket> bucketList = buckets.getBucketList();
                        if (CollectionUtils.isNotEmpty(bucketList)) {
                            if (topBkt.getLabel().equals("Medium")) {
                                long unexpectedBkts = bucketList.stream().filter(bkt -> "High".equals(bkt.getLabel()))
                                        .count();
                                Assert.assertEquals(unexpectedBkts, 0,
                                        "Should not have any High bkt when the top bkt is Medium");
                            } else if (topBkt.getLabel().equals("Normal")) {
                                long unexpectedBkts = bucketList.stream()
                                        .filter(bkt -> "High".equals(bkt.getLabel()) || "Medium".equals(bkt.getLabel()))
                                        .count();
                                Assert.assertEquals(unexpectedBkts, 0,
                                        "Should not have any High or Medium bkt when the top bkt is Normal");
                            }
                        }
                    }
                }
            }
        });
    }

    private void verifyTechTopN(CategoryTopNTree catTopNTree, StatsCube cube) {
        catTopNTree.getSubcategories().values().forEach(attrs -> {
            Long previousCount = null;
            for (TopAttribute attr : attrs) {
                Bucket topBkt = attr.getTopBkt();
                if (topBkt != null) {
                    if (topBkt.getLabel().equals("Yes")) {
                        Long currentCount = topBkt.getCount();
                        if (previousCount != null) {
                            Assert.assertTrue(currentCount <= previousCount, String.format(
                                    "Current count %d is bigger than previous count %d", currentCount, previousCount));
                        }
                        previousCount = currentCount;
                    }

                    AttributeStats attributeStats = cube.getStatistics().get(attr.getAttribute());
                    Buckets buckets = attributeStats.getBuckets();
                    if (buckets != null) {
                        List<Bucket> bucketList = buckets.getBucketList();
                        if (CollectionUtils.isNotEmpty(bucketList)) {
                            if (topBkt.getLabel().equals("No")) {
                                long unexpectedBkts = bucketList.stream().filter(bkt -> "Yes".equals(bkt.getLabel()))
                                        .count();
                                Assert.assertEquals(unexpectedBkts, 0,
                                        "Should not have any Yes bkt when the top bkt is No");
                            }
                        }
                    }
                }
            }
        });
    }

    private void verifyPurchaseHistoryTopN(CategoryTopNTree catTopNTree) {
        catTopNTree.getSubcategories().entrySet().forEach(entry -> {
            if (!"Other".equalsIgnoreCase(entry.getKey())) {
                TopAttribute firstAttr = entry.getValue().get(0);
                Assert.assertTrue(firstAttr.getAttribute().endsWith(StatsCubeUtils.HASEVER_PURCHASED_SUFFIX),
                        String.format("The first attribute is not a \"Has Ever Purchased\" attribute, but instead %s",
                                firstAttr.getAttribute()));
            }
        });
    }

    private void verifyTopBkt(StatsCube cube, String attribute, Bucket topBkt, Comparator<Bucket> comparator) {
        AttributeStats attributeStats = cube.getStatistics().get(attribute);
        Bucket expectedTopBkt = attributeStats.getBuckets().getBucketList().stream().sorted(comparator).findFirst()
                .orElse(null);
        assertSameBucket(topBkt, expectedTopBkt);
    }

    private void assertSameBucket(Bucket bkt1, Bucket bkt2) {
        Assert.assertEquals(bkt1.getLabel(), bkt2.getLabel());
        Assert.assertEquals(bkt1.getCount(), bkt2.getCount());
    }

    private Iterator<GenericRecord> readAvro() throws IOException {
        InputStream avroIs = readResource("amstats.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(avroIs);
        return records.iterator();
    }

    private InputStream readResource(String fileName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE_ROOT + fileName);
    }

}
