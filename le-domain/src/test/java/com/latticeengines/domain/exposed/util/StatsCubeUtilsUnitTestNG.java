package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;

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
    public void testTopN() throws Exception {
        InputStream is = readResource("statscubes.json.gz");
        GZIPInputStream gis = new GZIPInputStream(is);
        Map<String, StatsCube> cubes = JsonUtils.deserialize(gis, new TypeReference<Map<String, StatsCube>>() {
        });

        Map<String, List<ColumnMetadata>> cmMap = new HashMap<>();
        for (BusinessEntity entity : Arrays.asList( //
                BusinessEntity.Account, //
                BusinessEntity.Contact, //
                BusinessEntity.PurchaseHistory)) {
            String role = entity.getServingStore().name();
            is = readResource(role + ".json.gz");
            gis = new GZIPInputStream(is);
            Table table = JsonUtils.deserialize(gis, Table.class);
            cmMap.put(entity.name(), table.getColumnMetadata());
        }

        is = readResource("productData.json");
        DataPage dataPage = JsonUtils.deserialize(is, DataPage.class);
        Map<String, String> productMap = new HashMap<>();
        dataPage.getData().forEach(row -> productMap.put( //
                (String) row.get(InterfaceName.ProductId.name()), //
                (String) row.get(InterfaceName.ProductName.name()) //
        ));

        TopNTree topNTree = StatsCubeUtils.constructTopNTree(cubes, cmMap, true);
//         StatsCubeUtils.processPurchaseHistoryCategory(topNTree, productMap);
        // JsonUtils.serialize(topNTree, new FileOutputStream(new File("tree.json")));

        verifyDateAttrInTopN(topNTree, cmMap);
        StatsCube cube = StatsCubeUtils.retainTop5Bkts(cubes.get(BusinessEntity.Account.name()));
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
            int previousBktId = -1;
            for (TopAttribute attr : attrs) {
                Bucket topBkt = attr.getTopBkt();
                if (topBkt != null) {
                    Long currentCount = topBkt.getCount();
                    int currentBktId = topBkt.getId().intValue();
                    if (currentBktId != previousBktId && previousBktId != -1) {
                        Assert.assertTrue(currentBktId < previousBktId,
                                String.format("%s: Current bkt id %d is smaller than previous id %d.",
                                        attr.getAttribute(), currentBktId, previousBktId));
                    } else if (previousCount != null) {
                        Assert.assertTrue(currentCount <= previousCount,
                                String.format("%s: Current count %d is bigger than previous count %d",
                                        attr.getAttribute(), currentCount, previousCount));
                    }
                    previousCount = currentCount;
                    previousBktId = currentBktId;

                    AttributeStats attributeStats = cube.getStatistics().get(attr.getAttribute());
                    Buckets buckets = attributeStats.getBuckets();
                    if (buckets != null) {
                        List<Bucket> bucketList = buckets.getBucketList();
                        if (CollectionUtils.isNotEmpty(bucketList)) {
                            if (topBkt.getLabel().equals("Medium")) {
                                long unexpectedBkts = bucketList.stream().filter(bkt -> "High".equals(bkt.getLabel()))
                                        .count();
                                Assert.assertEquals(unexpectedBkts, 0, attr.getAttribute()
                                        + ": Should not have any High bkt when the top bkt is Medium");
                            } else if (topBkt.getLabel().equals("Normal")) {
                                long unexpectedBkts = bucketList.stream()
                                        .filter(bkt -> "High".equals(bkt.getLabel()) || "Medium".equals(bkt.getLabel()))
                                        .count();
                                Assert.assertEquals(unexpectedBkts, 0, attr.getAttribute()
                                        + ": Should not have any High or Medium bkt when the top bkt is Normal");
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
                                        attr.getAttribute() + ": Should not have any Yes bkt when the top bkt is No");
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

    private List<ColumnMetadata> prepareMockMetadata() {
        List<ColumnMetadata> cms = new ArrayList<>();
        ColumnMetadata cm1 = new ColumnMetadata();
        cm1.setColumnId("AlexaOnlineSince");
        cm1.setFundamentalType(FundamentalType.DATE);
        cm1.setCategory(Category.ONLINE_PRESENCE);
        ColumnMetadata cm2 = new ColumnMetadata();
        cm2.setColumnId("LastModifiedDate");
        cm2.setLogicalDataType(LogicalDataType.Date);
        cm2.setCategory(Category.ACCOUNT_ATTRIBUTES);
        cms.add(cm1);
        cms.add(cm2);
        return cms;
    }

    private void verifyDateAttrInTopN(TopNTree topNTree, Map<String, List<ColumnMetadata>> cmMap) {
        Map<AttributeLookup, ColumnMetadata> consolidatedCmMap = new HashMap<>();
        cmMap.forEach((name, cms) -> {
            BusinessEntity entity = BusinessEntity.valueOf(name);
            cms.forEach(cm -> {
                AttributeLookup attributeLookup = new AttributeLookup(entity, cm.getAttrName());
                consolidatedCmMap.put(attributeLookup, cm);
            });
        });

        topNTree.getCategories().forEach(((category, categoryTopNTree) -> //
        categoryTopNTree.getSubcategories().forEach((subCat, topAttrs) -> { //
            topAttrs.forEach(topAttr -> {
                AttributeLookup attributeLookup = new AttributeLookup(topAttr.getEntity(), topAttr.getAttribute());
                ColumnMetadata cm = consolidatedCmMap.get(attributeLookup);
                Assert.assertNotEquals(cm.getFundamentalType(), FundamentalType.DATE);
                Assert.assertNotEquals(cm.getLogicalDataType(), LogicalDataType.Timestamp);
                Assert.assertNotEquals(cm.getLogicalDataType(), LogicalDataType.Date);
            });
        })));
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
