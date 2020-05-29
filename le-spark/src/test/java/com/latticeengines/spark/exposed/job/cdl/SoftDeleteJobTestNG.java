package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TimeRanges;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.UserId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.spark.DeleteUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

public class SoftDeleteJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteJobTestNG.class);

    private static final List<Pair<String, Class<?>>> WEB_VISIT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(WebVisitDate.name(), Long.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(UserId.name(), String.class) //
    );

    private static final List<Pair<String, Class<?>>> DELETE_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(TimeRanges.name(), String.class) //
    );

    @Test(groups = "functional")
    private void test() {
        prepareTestData();
        SoftDeleteConfig config = new SoftDeleteConfig();
        config.setIdColumn(AccountId.name());
        config.setEventTimeColumn(WebVisitDate.name());
        config.setTimeRangesColumn(TimeRanges.name());

        SparkJobResult result = runSparkJob(SoftDeleteJob.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        // account id -> visit time
        Map<String, Set<Long>> visitTimes = new HashMap<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            counter.incrementAndGet();
            log.info(debugStr(record, WEB_VISIT_FIELDS.stream().map(Pair::getKey).collect(Collectors.toSet())));

            String accountId = record.get(AccountId.name()).toString();
            Long visitTime = (Long) record.get(WebVisitDate.name());
            visitTimes.putIfAbsent(accountId, new HashSet<>());
            visitTimes.get(accountId).add(visitTime);
        });

        log.info("Number of records = {}", counter.get());
        Assert.assertEquals(visitTimes, getExpectedVisitTimes());
        return true;
    }

    private Map<String, Set<Long>> getExpectedVisitTimes() {
        // account id -> visit time
        Map<String, Set<Long>> visitTimes = new HashMap<>();
        visitTimes.put("A1", Collections.singleton(1L));
        visitTimes.put("A2", Sets.newHashSet(1L, 2L, 100L));
        return visitTimes;
    }

    private void prepareTestData() {
        Object[][] account = new Object[][] { //
                { "A1", "Google", 1L, "https://dnb.com", "u1" }, // out of time range, not deleted
                { "A1", "Google", 10L, "https://dnb.com", "u2" }, //
                /*-
                 * A2 are not in ID list, not deleted
                 */
                { "A2", "Facebook", 1L, "https://dnb.com", "u3" }, //
                { "A2", "Facebook", 2L, "https://dnb.com", "u3" }, //
                { "A2", "Facebook", 100L, "https://dnb.com", "u3" }, //
                { "A3", "Netflix", 5000L, "https://dnb.com", "u4" }, //
        };
        uploadHdfsDataUnit(account, WEB_VISIT_FIELDS);

        Object[][] delete = new Object[][] { //
                { "A1", serializeTimeRanges(new long[][] { { 2L, 5L }, { 5L, 10L } }) }, //
                { "A3", serializeTimeRanges(new long[][] { { 2L, 5000L } }) }, //
        };
        uploadHdfsDataUnit(delete, DELETE_FIELDS);
    }

    private String serializeTimeRanges(long[][] timeRanges) {
        List<WrappedArray<Object>> ranges = Arrays.stream(timeRanges).map(WrappedArray::make)
                .collect(Collectors.toList());
        return DeleteUtils.serializeTimeRanges(JavaConversions.asScalaBuffer(ranges));
    }
}
