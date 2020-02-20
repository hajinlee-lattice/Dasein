package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.InternalId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDateId;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class AppendRawStreamJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AppendRawStreamJobTestNG.class);

    private static final String DATE_ATTR = InterfaceName.WebVisitDate.name();
    private static final String OPP_ID = "OpportunityId";
    private static final String Stage = "Stage";
    private static final List<Pair<String, Class<?>>> WEB_ACTIVITY_FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(EntityId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(DATE_ATTR, Long.class));
    private static final List<Pair<String, Class<?>>> WEB_ACTIVITY_MASTER_FIELDS = new ArrayList<>(WEB_ACTIVITY_FIELDS);

    static {
        WEB_ACTIVITY_MASTER_FIELDS.add(Pair.of(__StreamDate.name(), String.class));
        WEB_ACTIVITY_MASTER_FIELDS.add(Pair.of(__StreamDateId.name(), Integer.class));
    }

    private static final String ID_DAY_1 = "2";
    private static final String ID_DAY_2 = "3";
    private static final int RETENTION_DAYS = 7;
    private static final long now = LocalDate.of(2019, 11, 23) //
            .atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

    @Test(groups = "functional")
    private void test() {
        Pair<AppendRawStreamConfig, List<String>> testData = prepareTestData();

        SparkJobResult result = runSparkJob(AppendRawStreamJob.class, testData.getLeft(), testData.getRight(),
                getWorkspace());
        verifyResult(result);
    }

    @Test(groups = "functional")
    private void testWithDedup() {
        Pair<AppendRawStreamConfig, List<String>> testData = prepareTestDataWithDedup();

        SparkJobResult result = runSparkJob(AppendRawStreamJob.class, testData.getLeft(), testData.getRight(),
                getWorkspace());
        verify(result, Collections.singletonList(this::verifyDedupOutput));
    }

    private Pair<AppendRawStreamConfig, List<String>> prepareTestData() {
        // nDaysBefore > RETENTION_DAYS will be purged
        Object[][] matched = new Object[][]{ //
                testRow(0L, true, 8), //
                testRow(1L, true, 7), //
                testRow(2L, true, 10), //
                testRow(3L, true, 0), //
                testRow(4L, true, 2), //
                testRow(5L, true, 3), //
        };
        Object[][] master = new Object[][]{ //
                testRow(100L, false, 1), //
                testRow(101L, false, 8), //
                testRow(102L, false, 5), //
                testRow(103L, false, 7), //
        };

        List<String> inputs = Arrays.asList(uploadHdfsDataUnit(matched, WEB_ACTIVITY_FIELDS),
                uploadHdfsDataUnit(master, WEB_ACTIVITY_MASTER_FIELDS));

        AppendRawStreamConfig config = new AppendRawStreamConfig();
        config.currentEpochMilli = now;
        config.retentionDays = RETENTION_DAYS;
        config.matchedRawStreamInputIdx = 0;
        config.masterInputIdx = 1;
        config.dateAttr = DATE_ATTR;
        return Pair.of(config, inputs);
    }

    private Pair<AppendRawStreamConfig, List<String>> prepareTestDataWithDedup() {
        //
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(InternalId.name(), String.class),
                Pair.of(AccountId.name(), String.class),
                Pair.of(OPP_ID, String.class),
                Pair.of(Stage, String.class),
                Pair.of(DATE_ATTR, Long.class)
        );
        //
        Object[][] input = new Object[][]{ //
                testDedupRow("0", "acc1", "opp1", "open", 0, 1),
                testDedupRow("1", "acc1", "opp1", "dev", 0, 2),
                testDedupRow(ID_DAY_1, "acc2", "opp1", "won", 0, 3),
                testDedupRow(ID_DAY_2, "acc1", "opp2", "open", 1, 0)
        };
        AppendRawStreamConfig config = new AppendRawStreamConfig();
        config.currentEpochMilli = now;
        config.retentionDays = RETENTION_DAYS;
        config.matchedRawStreamInputIdx = 0;
        config.dateAttr = DATE_ATTR;
        config.reducer = prepareReducer();
        return Pair.of(config, Collections.singletonList(uploadHdfsDataUnit(input, fields)));
    }

    private Object[] testRow(long id, boolean isImport, int nDaysBeforeNow) {
        long time = Instant.ofEpochMilli(now).minus(nDaysBeforeNow, ChronoUnit.DAYS).toEpochMilli();
        String accountId = String.format("a%d", id);
        List<Object> row = Lists.newArrayList(id, accountId, accountId, String.format("Company %d", id),
                String.format("/contents/%d", id), time);
        if (!isImport) {
            String dateStr = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(time));
            Integer datePeriod = DateTimeUtils.dateToDayPeriod(dateStr);
            row.add(dateStr);
            row.add(datePeriod);
        }
        return row.toArray();
    }

    private Object[] testDedupRow(String id, String accId, String oppId, String stage, int nDaysAfter, int nHoursAfter) {
        long time = Instant.ofEpochMilli(now).plus(nDaysAfter, ChronoUnit.DAYS).plus(nHoursAfter, ChronoUnit.HOURS).toEpochMilli();
        List<Object> row = Arrays.asList(id, accId, oppId, stage, time);
        return row.toArray();
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        long minTime = Instant.ofEpochMilli(now).minus(RETENTION_DAYS, ChronoUnit.DAYS).toEpochMilli();
        Integer minDayPeriod = DateTimeUtils
                .dateToDayPeriod(DateTimeUtils.toDateOnlyFromMillis(String.valueOf(minTime)));
        Assert.assertNotNull(minDayPeriod);
        AtomicInteger count = new AtomicInteger();
        Set<Long> expectedIds = Sets.newHashSet(1L, 3L, 4L, 5L, 100L, 102L, 103L);
        Set<Long> ids = new HashSet<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            List<String> nameValues = WEB_ACTIVITY_MASTER_FIELDS.stream() //
                    .map(Pair::getKey) //
                    .map(field -> String.format("%s=%s", field, record.get(field))) //
                    .collect(Collectors.toList());
            // has internal ID and date attrs
            Assert.assertNotNull(record.get(InternalId.name()));
            Assert.assertNotNull(record.get(__StreamDate.name()));
            Assert.assertNotNull(record.get(DATE_ATTR));

            long timestamp = (Long) record.get(DATE_ATTR);
            Assert.assertTrue(minTime <= timestamp,
                    String.format("All processed stream should have timestamp within %d days", RETENTION_DAYS));
            String dateStr = record.get(__StreamDate.name()).toString();
            Integer datePeriod = DateTimeUtils.dateToDayPeriod(dateStr);
            Assert.assertNotNull(datePeriod);
            Assert.assertTrue(minDayPeriod <= datePeriod,
                    String.format("All processed stream should have date period after %d", minDayPeriod));

            ids.add((Long) record.get(InternalId.name()));
            count.incrementAndGet();
        });
        Assert.assertEquals(count.get(), 7);
        Assert.assertEquals(ids, expectedIds);
        return true;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(OPP_ID));
        reducer.setArguments(Collections.singletonList(DATE_ATTR));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private Boolean verifyDedupOutput(HdfsDataUnit output) {
        String[] fields = {InternalId.name(), AccountId.name(), OPP_ID, Stage, __StreamDate.name()};
        long oneDayAfter = Instant.ofEpochMilli(now).plus(1, ChronoUnit.DAYS).toEpochMilli();
        Object[][] outputVals = new Object[][]{
                {ID_DAY_1, "acc2", "opp1", "won", DateTimeUtils.toDateOnlyFromMillis(String.valueOf(now))},
                {ID_DAY_2, "acc1", "opp2", "open", DateTimeUtils.toDateOnlyFromMillis(String.valueOf(oneDayAfter))},
        };
        String[] filteredRecordIds = {ID_DAY_1, ID_DAY_2};
        Map<String, Map<String, Object>> expectedMap = new HashMap<>();
        for (int i = 0; i < filteredRecordIds.length; i++) {
            Map<String, Object> fieldMap = new HashMap<>();
            Object[] row = outputVals[i];
            String targetId = filteredRecordIds[i];
            for (int j = 0; j < fields.length; j++) {
                String fieldName = fields[j];
                fieldMap.put(fieldName, row[j]);
            }
            expectedMap.put(targetId, fieldMap);
        }
        Iterator<GenericRecord> iterator = verifyAndReadTarget(output);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            String recordId = record.get(InternalId.toString()).toString();
            Map<String, Object> expected = expectedMap.get(recordId); // fieldName -> value
            verifyFields(expected, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, expectedMap.size());
        return true;
    }

    private void verifyFields(Map<String, Object> expected, GenericRecord record) {
        expected.forEach((fieldName, val) -> {
            Assert.assertNotNull(record.get(fieldName));
            Assert.assertEquals(expected.get(fieldName).toString(), record.get(fieldName).toString());
        });
    }
}
