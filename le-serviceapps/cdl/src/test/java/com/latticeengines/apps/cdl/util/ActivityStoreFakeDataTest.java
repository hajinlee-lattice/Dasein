package com.latticeengines.apps.cdl.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * This test is for manually create fake activity metric data for M32
 */
public class ActivityStoreFakeDataTest {

    private static final String AM_ATTR_1 = "am_twv__abcdef123456__l_2_w";
    private static final String AM_ATTR_2 = "am_wvbsm__abcdef123456_123456abcdef__l_4_w";
    private static final String AM_ATTR_3 = "am_twv__abcdef123457__l_2_w";
    private static final String AM_ATTR_4 = "am_wvbsm__abcdef123457_123457abcdef__l_4_w";

    private static final Random random = new Random(System.currentTimeMillis());

    @Test(groups = "manual")
    public void generateWebVisitProfile() {
        String tenantId = "ysong_20191007";
        List<String> accountIds = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            accountIds.add(String.valueOf(i));
        }

        String tableName = tenantId + "_" + NamingUtils.timestamp(BusinessEntity.WebVisitProfile.name());
        List<Pair<String, Class<?>>> fieldTypes = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(AM_ATTR_1, Long.class), //
                Pair.of(AM_ATTR_2, Long.class), //
                Pair.of(AM_ATTR_3, Long.class), //
                Pair.of(AM_ATTR_4, Long.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class) //
        );
        Schema avroSchema = AvroUtils.constructSchema(tableName, fieldTypes);
        List<GenericRecord> records = getData(avroSchema, accountIds);

        StatsCube cube = getStatsCube(records);
        System.out.println(JsonUtils.pprint(cube));

        Table table = constructTable(tenantId, tableName, fieldTypes, records.size());
        System.out.println(JsonUtils.pprint(table));
    }

    private List<GenericRecord> getData(Schema avroSchema, List<String> accountIds) {
        List<GenericRecord> records = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        for (String accountId: accountIds) {
            GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
            builder.set(InterfaceName.AccountId.name(), accountId);

            builder.set(AM_ATTR_1, generateVisits());
            builder.set(AM_ATTR_2, generateVisits());
            builder.set(AM_ATTR_3, generateVisits());
            builder.set(AM_ATTR_4, generateVisits());

            builder.set(InterfaceName.CDLCreatedTime.name(), currentTime);
            builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
            records.add(builder.build());
        }
        return records;
    }

    private StatsCube getStatsCube(List<GenericRecord> records) {
        Map<String, AttributeStats> statsMap = new HashMap<>();
        statsMap.put(AM_ATTR_1, calcAttrStats(AM_ATTR_1, records, records.size()));
        statsMap.put(AM_ATTR_2, calcAttrStats(AM_ATTR_2, records, records.size()));
        statsMap.put(AM_ATTR_3, calcAttrStats(AM_ATTR_3, records, records.size()));
        statsMap.put(AM_ATTR_4, calcAttrStats(AM_ATTR_4, records, records.size()));

        StatsCube cube = new StatsCube();
        cube.setCount((long) records.size());
        cube.setStatistics(statsMap);
        return cube;
    }

    private AttributeStats calcAttrStats(String fieldName, List<GenericRecord> records, long cnt) {
        AttributeStats attrStats = new AttributeStats();
        attrStats.setNonNullCount(cnt);
        attrStats.setBuckets(collectBuckets(records, fieldName));
        return attrStats;
    }

    private Buckets collectBuckets(List<GenericRecord> records, String fieldName) {
        AtomicLong bkt1Cnt = new AtomicLong(0); // 1-9
        AtomicLong bkt2Cnt = new AtomicLong(0); // 10-99
        AtomicLong bkt3Cnt = new AtomicLong(0); // 100-999
        AtomicLong bkt4Cnt = new AtomicLong(0); // >=1000

        for (GenericRecord record: records) {
            long val = (long) record.get(fieldName);
            if (val < 10) {
                bkt1Cnt.incrementAndGet();
            } else if (val < 100) {
                bkt2Cnt.incrementAndGet();
            } else if (val < 1000) {
                bkt3Cnt.incrementAndGet();
            } else {
                bkt4Cnt.incrementAndGet();
            }
        }

        Bucket bkt1 = Bucket.rangeBkt(null, 10);
        bkt1.setId(1L);
        bkt1.setLabel("< 10");
        bkt1.setCount(bkt1Cnt.get());
        Bucket bkt2 = Bucket.rangeBkt(10, 100);
        bkt2.setId(2L);
        bkt2.setLabel("10 - 100");
        bkt2.setCount(bkt1Cnt.get());
        Bucket bkt3 = Bucket.rangeBkt(100, 1000);
        bkt3.setId(3L);
        bkt3.setLabel("100 - 1000");
        bkt3.setCount(bkt1Cnt.get());
        Bucket bkt4 = Bucket.rangeBkt(1000, null);
        bkt4.setId(4L);
        bkt4.setLabel(">= 1000");
        bkt4.setCount(bkt1Cnt.get());

        Buckets bkts = new Buckets();
        bkts.setType(BucketType.Numerical);
        bkts.setBucketList(Arrays.asList(bkt1, bkt2, bkt3, bkt4));
        return bkts;
    }

    private Table constructTable(String tenantId, String tableName, List<Pair<String, Class<?>>> fields, long count) {
        Extract extract = new Extract();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        String hdfsPath = PathBuilder.buildDataTablePath("QA", customerSpace).append(tableName).toString();
        extract.setExtractionTimestamp(System.currentTimeMillis());
        extract.setPath(hdfsPath);
        extract.setProcessedRecords(count);
        extract.setName(NamingUtils.timestamp("Extract"));
        Table table = new Table();
        table.setName(tableName);
        table.setDisplayName(tableName);
        table.setAttributes(getAttributes(fields));
        table.setTableType(TableType.DATATABLE);
        table.setExtracts(Collections.singletonList(extract));
        return table;
    }

    private List<Attribute> getAttributes(List<Pair<String, Class<?>>> fields) {
        List<Attribute> attributes = new ArrayList<>();
        for (Pair<String, Class<?>> field: fields) {
            String attrName = field.getLeft();

            Attribute attribute = new Attribute(attrName);
            attribute.setPhysicalDataType(field.getRight().getSimpleName());
            attribute.setCategory(Category.WEB_VISIT_PROFILE);
            switch (attrName) {
                case AM_ATTR_1:
                    attribute.setDisplayName("Visited in last 2 weeks");
                    attribute.setDescription("Number of visits on a url matching Path 1 in last 2 weeks");
                    attribute.setSubcategory("Path 1");
                    break;
                case AM_ATTR_2:
                    attribute.setDisplayName("Visited from Source 1 in last 4 weeks");
                    attribute.setDescription("Number of visits on a url matching Path 1 from Source 1 in last 4 weeks");
                    attribute.setSubcategory("Path 1");
                    break;
                case AM_ATTR_3:
                    attribute.setDisplayName("Visited in last 2 weeks");
                    attribute.setDescription("Number of visits on a url matching Path 2 in last 2 weeks");
                    attribute.setSubcategory("Path 2");
                    break;
                case AM_ATTR_4:
                    attribute.setDisplayName("Visited from Source 2 in last 4 weeks");
                    attribute.setDescription("Number of visits on a url matching Path 2 from Source 2 in last 4 weeks");
                    attribute.setSubcategory("Path 2");
                    break;
                default:
            }

            attributes.add(attribute);
        }
        return attributes;
    }

    /**
     * 1-9: 50%
     * 10-99: 25%
     * 100-999: 15%
     * >=1000: 10%
     */
    private long generateVisits() {
        int rnd = random.nextInt(100);
        if (rnd < 50) {
            return random.nextInt(10);
        } else if (rnd < 75) {
            return 10 + random.nextInt(90);
        } else if (rnd < 90) {
            return 100 + random.nextInt(900);
        } else {
            return 1000 + random.nextInt(10000);
        }
    }

}
