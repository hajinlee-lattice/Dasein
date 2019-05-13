package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket.Change;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;

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
        Assert.assertTrue(attrStats.getNonNullCount() <= maxCount, //
                attrName + JsonUtils.pprint(attrStats)));
    }

    @Test(groups = "unit")
    public void testSortRating() throws Exception {
        StatsCube cube = new StatsCube();
        Map<String, AttributeStats> attrStats = new HashMap<>();

        AttributeStats stats = new AttributeStats();
        Buckets buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        List<Bucket> bucketList = new ArrayList<>();
        Bucket bktB = Bucket.valueBkt("B");
        bktB.setId(1L);
        bucketList.add(bktB);

        Bucket bktC = Bucket.valueBkt("C");
        bktC.setId(2L);
        bucketList.add(bktC);

        Bucket bktA = Bucket.valueBkt("A");
        bktA.setId(3L);
        bucketList.add(bktA);

        buckets.setBucketList(bucketList);
        stats.setBuckets(buckets);

        attrStats.put("Attr", stats);
        cube.setStatistics(attrStats);

        List<Bucket> unsortedList = cube.getStatistics().get("Attr").getBuckets().getBucketList();
        Assert.assertEquals(unsortedList.get(0).getLabel(), "B");
        Assert.assertEquals(unsortedList.get(1).getLabel(), "C");
        Assert.assertEquals(unsortedList.get(2).getLabel(), "A");

        StatsCubeUtils.sortRatingBuckets(stats);

        List<Bucket> sortedList = cube.getStatistics().get("Attr").getBuckets().getBucketList();
        Assert.assertEquals(sortedList.get(0).getLabel(), "A");
        Assert.assertEquals(sortedList.get(1).getLabel(), "B");
        Assert.assertEquals(sortedList.get(2).getLabel(), "C");
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
            List<ColumnMetadata> cms = table.getColumnMetadata().stream() //
                    .peek(cm -> cm.enableGroup(ColumnSelection.Predefined.Segment)) //
                    .collect(Collectors.toList());
            cmMap.put(entity.name(), cms);
        }

        TopNTree topNTree = StatsCubeUtils.constructTopNTree(cubes, cmMap, true, //
                ColumnSelection.Predefined.Segment, false);
        verifyCategoryOrder(topNTree);
        verifyDateAttrInTopN(topNTree, cmMap);
    }

    @Test(groups = "unit", dataProvider = "BktsToChgBkts")
    public void testBktToChgBkt(int id, Object min, Object max, boolean minInclusive, boolean maxInclusive,
            boolean isValBkt, Bucket.Change.Direction direction, Bucket.Change.ComparisonType comparisonType,
            Object val1, Object val2) {
        Bucket bkt;
        if (isValBkt) {
            bkt = Bucket.valueBkt(String.valueOf(min));
        } else {
            bkt = Bucket.rangeBkt(min, max, minInclusive, maxInclusive);
        }
        Bucket chgBkt = StatsCubeUtils.convertBucketToChgBucket(bkt);
        Assert.assertEquals(chgBkt.getChange().getDirection(), direction);
        Assert.assertEquals(chgBkt.getChange().getComparisonType(), comparisonType);
        Assert.assertEquals(chgBkt.getChange().getAbsVals().get(0), val1);
        if (val2 != null) {
            Assert.assertNotNull(chgBkt.getChange().getAbsVals().get(1));
            Assert.assertEquals(chgBkt.getChange().getAbsVals().get(1), val2);
        }
    }

    @Test(groups = "unit", dataProvider = "ChgBktsToBkts")
    public void testChgBktToBkt(int id, Bucket.Change.Direction direction, Bucket.Change.ComparisonType chgCmp,
            Object chgVal1, Object chgVal2, ComparisonType cmp, Object val1, Object val2) {
        Bucket chgBkt = new Bucket();
        Change chg = new Change();
        chg.setDirection(direction);
        chg.setComparisonType(chgCmp);
        List<Object> absVals = new ArrayList<>();
        absVals.add(chgVal1);
        if (chgVal2 != null) {
            absVals.add(chgVal2);
        }
        chg.setAbsVals(absVals);
        chgBkt.setChange(chg);
        Bucket bkt = StatsCubeUtils.convertChgBucketToBucket(chgBkt);
        Assert.assertEquals(bkt.getComparisonType(), cmp);
        Assert.assertEquals(bkt.getValues().get(0), val1);
        if (val2 != null) {
            Assert.assertNotNull(bkt.getValues().get(1));
            Assert.assertEquals(bkt.getValues().get(1), val2);
        }
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

                // Account Attributes (aka "My Attributes") and Contact Attributes that are date attribute types are
                // allowed to be in the TopN Tree as long as they are not system attributes.
                if (!(Category.ACCOUNT_ATTRIBUTES.equals(cm.getCategory())
                        || Category.CONTACT_ATTRIBUTES.equals(cm.getCategory()))
                        || StatsCubeUtils.isSystemAttribute(topAttr.getEntity(), cm, false)) {
                    Assert.assertNotEquals(cm.getFundamentalType(), FundamentalType.DATE);
                    Assert.assertNotEquals(cm.getLogicalDataType(), LogicalDataType.Timestamp);
                    Assert.assertNotEquals(cm.getLogicalDataType(), LogicalDataType.Date);
                } else if ("AccountCreatedDate".equals(cm.getAttrName())) {
                    Assert.assertEquals(cm.getLogicalDataType(), LogicalDataType.Date);
                } else if ("LastCalledDate".equals(cm.getAttrName())) {
                    Assert.assertEquals(cm.getFundamentalType(), FundamentalType.DATE);
                }
            });
        })));
    }

    private void verifyCategoryOrder(TopNTree topNTree) {
        List<Category> categories = new ArrayList<>(topNTree.getCategories().keySet());
        for (int i = 1; i < categories.size(); i++) {
            Assert.assertTrue(categories.get(i).getOrder() > categories.get(i - 1).getOrder());
        }
    }

    private Iterator<GenericRecord> readAvro() throws IOException {
        InputStream avroIs = readResource("amstats.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(avroIs);
        return records.iterator();
    }

    private InputStream readResource(String fileName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE_ROOT + fileName);
    }

    // Bucket: ID, min, max, minInclusive, maxInclusive, isValBkt
    // ChgBucket: Direction, ComparisonType, Val1, Val2
    @DataProvider(name = "BktsToChgBkts")
    private Object[][] bktsToChgBkts() {
        return new Object[][] { //
                // Numerical INC
                { 0, 0, 3, true, false, false, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.BETWEEN, 0.0,
                        3.0 }, //
                { 1, 1, 3, true, false, false, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.BETWEEN, 1.0,
                        3.0 }, //
                { 2, 3, null, true, false, false, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AT_LEAST,
                        3.0, null }, //
                { 3, null, 3, true, false, false, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                        3.0, null }, //
                { 4, 0, null, true, false, false, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AT_LEAST,
                        0.0, null }, //

                // Numerical DEC
                { 5, null, 0, true, false, false, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AT_LEAST,
                        0.0, null }, //
                { 6, -3, 0, true, false, false, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.BETWEEN, 0.0,
                        3.0 }, //
                { 7, -3, -1, true, false, false, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.BETWEEN, 1.0,
                        3.0 }, //
                { 8, null, -3, true, false, false, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AT_LEAST,
                        3.0, null }, //
                { 9, -3, null, true, false, false, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                        3.0, null }, //

                // Distinct INC
                { 10, 3, 3, false, false, true, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.BETWEEN, 3.0,
                        3.0 }, //
                { 11, 0, 0, false, false, true, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.BETWEEN, 0.0,
                        0.0 }, //

                // Distinct DEC
                { 12, -3, -3, false, false, true, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.BETWEEN,
                        3.0, 3.0 }, //
        };
    }

    // ChgBucket: ID, Direction, ComparisonType, Val1, Val2
    // Bucket: ComparisonType, Val1, Val2
    @DataProvider(name = "ChgBktsToBkts")
    private Object[][] chgBktsToBkts() {
        return new Object[][] {
                // INC
                { 0, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AS_MUCH_AS, 0, null,
                        ComparisonType.GTE_AND_LTE, 0.0, 0.0 }, //
                { 1, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AS_MUCH_AS, 3, null,
                        ComparisonType.GTE_AND_LTE, 0.0, 3.0 }, //
                { 2, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.BETWEEN, 0, 0,
                        ComparisonType.GTE_AND_LTE, 0.0, 0.0 }, //
                { 3, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.BETWEEN, 0, 3,
                        ComparisonType.GTE_AND_LTE, 0.0, 3.0 }, //
                { 4, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AT_LEAST, 0, null,
                        ComparisonType.GREATER_OR_EQUAL, 0.0, null }, //
                { 5, Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AT_LEAST, 3, null,
                        ComparisonType.GREATER_OR_EQUAL, 3.0, null }, //

                // DEC
                { 6, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS, 0, null,
                        ComparisonType.GTE_AND_LTE, 0.0, 0.0 }, //
                { 7, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS, 3, null,
                        ComparisonType.GTE_AND_LTE, -3.0, 0.0 }, //
                { 8, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.BETWEEN, 0, 0,
                        ComparisonType.GTE_AND_LTE, 0.0, 0.0 }, //
                { 9, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.BETWEEN, 0, 3,
                        ComparisonType.GTE_AND_LTE, -3.0, 0.0 }, //
                { 10, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AT_LEAST, 0, null,
                        ComparisonType.LESS_OR_EQUAL, 0.0, null }, //
                { 11, Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AT_LEAST, 3, null,
                        ComparisonType.LESS_OR_EQUAL, -3.0, null }, //
        };
    }

}
