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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class AppendRawStreamJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AppendRawStreamJobTestNG.class);

    private static final String DATE_ATTR = InterfaceName.WebVisitDate.name();
    private static final List<Pair<String, Class<?>>> IMPORT_FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(EntityId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(DATE_ATTR, Long.class));
    private static final List<Pair<String, Class<?>>> MASTER_FIELDS = new ArrayList<>(IMPORT_FIELDS);
    static {
        MASTER_FIELDS.add(Pair.of(__StreamDate.name(), String.class));
        MASTER_FIELDS.add(Pair.of(__StreamDateId.name(), Integer.class));
    }
    private static final int RETENTION_DAYS = 7;
    private static final long now = LocalDate.of(2019, 11, 23) //
            .atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

    @Test(groups = "functional")
    private void test() {
        Pair<AppendRawStreamConfig, List<String>> testData = prepareTestData();

        SparkJobResult result = runSparkJob(AppendRawStreamJob.class, testData.getLeft(), testData.getRight(),
                getWorkspace());
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    private Pair<AppendRawStreamConfig, List<String>> prepareTestData() {
        // nDaysBefore > RETENTION_DAYS will be purged
        Object[][] matched = new Object[][] { //
                testRow(0L, true, 8), //
                testRow(1L, true, 7), //
                testRow(2L, true, 10), //
                testRow(3L, true, 0), //
                testRow(4L, true, 2), //
                testRow(5L, true, 3), //
        };
        Object[][] master = new Object[][] { //
                testRow(100L, false, 1), //
                testRow(101L, false, 8), //
                testRow(102L, false, 5), //
                testRow(103L, false, 7), //
        };

        // TODO upload master store with partitioned data
        List<String> inputs = Arrays.asList(uploadHdfsDataUnit(matched, IMPORT_FIELDS),
                uploadHdfsDataUnit(master, MASTER_FIELDS));

        AppendRawStreamConfig config = new AppendRawStreamConfig();
        config.currentEpochMilli = now;
        config.retentionDays = RETENTION_DAYS;
        config.matchedRawStreamInputIdx = 0;
        config.masterInputIdx = 1;
        config.dateAttr = DATE_ATTR;
        return Pair.of(config, inputs);
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
            List<String> nameValues = MASTER_FIELDS.stream() //
                    .map(Pair::getKey) //
                    .map(field -> String.format("%s=%s", field, record.get(field))) //
                    .collect(Collectors.toList());
            log.info(Strings.join(nameValues, ","));
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
}
