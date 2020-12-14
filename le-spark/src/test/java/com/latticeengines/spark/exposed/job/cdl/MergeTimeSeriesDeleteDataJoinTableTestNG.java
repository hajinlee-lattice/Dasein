package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.UserId;
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

public class MergeTimeSeriesDeleteDataJoinTableTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MergeTimeSeriesDeleteDataJoinTableTestNG.class);

    private static final List<Pair<String, Class<?>>> DELETE_BY_ACC_INPUT_SCHEMA = Collections
            .singletonList(Pair.of(AccountId.name(), String.class));

    private static final List<Pair<String, Class<?>>> DELETE_BY_CON_INPUT_SCHEMA = Collections
            .singletonList(Pair.of(ContactId.name(), String.class));

    private static final List<Pair<String, Class<?>>> CONSOLIDATE_CONTACT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(UserId.name(), String.class) //
    );

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(this::verifyMergedDeleteData);
    }

    private Boolean verifyMergedDeleteData(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, Set<List<Long>>> expectedRanges = expectedTimeRanges();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            counter.incrementAndGet();
            log.info(debugStr(record, asList(ContactId.name(), InterfaceName.TimeRanges.name())));

            String conId = record.get(ContactId.name()).toString();
            String timeRanges = record.get(InterfaceName.TimeRanges.name()).toString();
            Assert.assertTrue(StringUtils.isNotBlank(conId), "contact id should not be blank");
            Assert.assertTrue(StringUtils.isNotBlank(timeRanges), "time ranges column should not be blank");

            Option<long[][]> parsedRanges = DeleteUtils.deserializeTimeRanges(timeRanges);
            Assert.assertTrue(parsedRanges.isDefined());
            Set<List<Long>> ranges = Arrays.stream(parsedRanges.get()).map(range -> Arrays.asList(range[0], range[1]))
                    .collect(Collectors.toSet());

            Assert.assertNotNull(ranges);
            Assert.assertEquals(ranges, expectedRanges.get(conId),
                    String.format("Time ranges for contact id %s doesn't match the expected value", conId));
        });
        log.info("Number of records = {}", counter.get());
        return true;
    }

    private Map<String, Set<List<Long>>> expectedTimeRanges() {
        Map<String, Set<List<Long>>> expectedTimeRanges = new HashMap<>();
        expectedTimeRanges.put("C1", ImmutableSet.of(asList(123L, 234L), asList(220L, 1000L)));
        expectedTimeRanges.put("C2", ImmutableSet.of(asList(123L, 234L), asList(220L, 1000L)));
        expectedTimeRanges.put("C3", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(123L, 234L)));
        expectedTimeRanges.put("C4", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(123L, 234L)));
        expectedTimeRanges.put("C5", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(123L, 234L)));
        expectedTimeRanges.put("C6", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE)));
        expectedTimeRanges.put("C7", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE), asList(123L, 234L)));
        expectedTimeRanges.put("C8", ImmutableSet.of(asList(Long.MIN_VALUE, Long.MAX_VALUE)));
        return expectedTimeRanges;
    }

    @Test(groups = "functional")
    private void test() {
        prepareConsolidContactData();

        MergeTimeSeriesDeleteDataConfig config = new MergeTimeSeriesDeleteDataConfig();
        config.joinKey = ContactId.name();
        config.timeRanges.put(0, asList(123L, 234L));
        config.timeRanges.put(2, asList(220L, 1000L));
        config.deleteIDs.put(0, AccountId.name());
        config.deleteIDs.put(1, AccountId.name());
        config.deleteIDs.put(2, AccountId.name());
        config.deleteIDs.put(3, ContactId.name());
        config.joinTableIdx = 4;

        SparkJobResult result = runSparkJob(MergeTimeSeriesDeleteData.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    private void prepareConsolidContactData() {

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
                { "A4" }, //
                { "A4" }, //
        };
        uploadHdfsDataUnit(input2, DELETE_BY_ACC_INPUT_SCHEMA);

        Object[][] input3 = new Object[][] { { "A1" }, //
                { "A5" }, //
        };
        uploadHdfsDataUnit(input3, DELETE_BY_ACC_INPUT_SCHEMA);

        Object[][] input4 = new Object[][] { { "C5" }, //
                { "C8" }, //
        };
        uploadHdfsDataUnit(input4, DELETE_BY_CON_INPUT_SCHEMA);

        Object[][] consolidateContact = new Object[][] { //
                { "A1", "C1", "Other" }, { "A1", "C2", "Other" }, { "A2", "C3", "Other" }, { "A2", "C4", "Other" },
                { "A2", "C5", "Other" }, { "A3", "C6", "Other" }, { "A4", "C7", "Other" } };
        uploadHdfsDataUnit(consolidateContact, CONSOLIDATE_CONTACT_FIELDS);
    }
}
