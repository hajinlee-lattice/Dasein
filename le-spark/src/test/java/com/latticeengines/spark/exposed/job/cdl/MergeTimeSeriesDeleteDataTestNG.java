package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static java.util.Arrays.asList;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeTimeSeriesDeleteDataConfig;
import com.latticeengines.spark.DeleteUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

import scala.Option;

public class MergeTimeSeriesDeleteDataTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MergeTimeSeriesDeleteDataTestNG.class);

    private static final List<Pair<String, Class<?>>> DELETE_BY_ACC_INPUT_SCHEMA = Collections
            .singletonList(Pair.of(AccountId.name(), String.class));

    @Test(groups = "functional")
    private void test() {
        prepareTestData();

        MergeTimeSeriesDeleteDataConfig config = new MergeTimeSeriesDeleteDataConfig();
        config.joinKey = AccountId.name();
        config.numberOfDeleteInputs = 3;
        config.timeRanges.put(0, asList(123L, 234L));
        config.timeRanges.put(2, asList(220L, 1000L));

        SparkJobResult result = runSparkJob(MergeTimeSeriesDeleteData.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(this::verifyMergedDeleteData);
    }

    private Boolean verifyMergedDeleteData(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, Set<List<Long>>> expectedRanges = expectedTimeRanges();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            counter.incrementAndGet();
            log.info(debugStr(record, asList(AccountId.name(), InterfaceName.TimeRanges.name())));

            String accId = record.get(AccountId.name()).toString();
            String timeRanges = record.get(InterfaceName.TimeRanges.name()).toString();
            Assert.assertTrue(StringUtils.isNotBlank(accId), "account id should not be blank");
            Assert.assertTrue(StringUtils.isNotBlank(timeRanges), "time ranges column should not be blank");

            Option<long[][]> parsedRanges = DeleteUtils.deserializeTimeRanges(timeRanges);
            Assert.assertTrue(parsedRanges.isDefined());
            Set<List<Long>> ranges = Arrays.stream(parsedRanges.get()).map(range -> Arrays.asList(range[0], range[1]))
                    .collect(Collectors.toSet());

            Assert.assertNotNull(ranges);
            Assert.assertEquals(ranges, expectedRanges.get(accId),
                    String.format("Time ranges for account id %s doesn't match the expected value", accId));
        });
        log.info("Number of records = {}", counter.get());
        return true;
    }

    private Map<String, Set<List<Long>>> expectedTimeRanges() {
        Map<String, Set<List<Long>>> expectedTimeRanges = new HashMap<>();
        expectedTimeRanges.put("A1", ImmutableSet.of(asList(123L, 234L), asList(220L, 1000L)));
        expectedTimeRanges.put("A2", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(123L, 234L)));
        expectedTimeRanges.put("A3", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE)));
        expectedTimeRanges.put("A4", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(123L, 234L)));
        expectedTimeRanges.put("A5", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(220L, 1000L)));
        return expectedTimeRanges;
    }

    private void prepareTestData() {
        Object[][] input1 = new Object[][] { //
                { "A1" }, //
                { "A2" }, //
                { "A4" }, //
                { "A4" }, //
                { "A4" }, //
        };
        uploadHdfsDataUnit(input1, DELETE_BY_ACC_INPUT_SCHEMA);

        Object[][] input2 = new Object[][] { //
                { "A2" }, //
                { "A3" }, //
                { "A4" }, // IDs should be deduped
                { "A4" }, //
                { "A5" }, //
                { "A5" }, //
        };
        uploadHdfsDataUnit(input2, DELETE_BY_ACC_INPUT_SCHEMA);

        Object[][] input3 = new Object[][] { //
                { "A1" }, //
                { "A5" }, //
        };
        uploadHdfsDataUnit(input3, DELETE_BY_ACC_INPUT_SCHEMA);
    }

}
