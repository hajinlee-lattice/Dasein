package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLUpdatedTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityLastUpdatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfContacts;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeCuratedAttributesConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeCuratedAttributesTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MergeCuratedAttributesTestNG.class);

    // input schema
    private static final List<Pair<String, Class<?>>> ACC_BATCH_STORE_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CDLUpdatedTime.name(), Long.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(CustomerAccountId.name(), String.class) //
    );
    private static final List<Pair<String, Class<?>>> LAST_ACTIVITY_DATE_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class) //
    );
    private static final List<Pair<String, Class<?>>> NUM_CONTACT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(NumberOfContacts.name(), Long.class) //
    );

    private static final List<Pair<String, Class<?>>> OUTPUT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class), //
            Pair.of(EntityLastUpdatedDate.name(), Long.class), //
            Pair.of(NumberOfContacts.name(), Long.class) //
    );

    // TODO add other test cases
    @Test(groups = "functional")
    private void test() {
        MergeCuratedAttributesConfig config = baseConfig();
        prepareTestData();
        log.info("Config = {}", JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(MergeCuratedAttributes.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            counter.incrementAndGet();
            log.info(debugStr(record, OUTPUT_FIELDS.stream().map(Pair::getKey).collect(Collectors.toSet())));
        });
        log.info("Number of records = {}", counter.get());
        // TODO add assertion
        return true;
    }

    private void prepareTestData() {
        // AccountId, LastActivityDate
        Object[][] lastActivityDate = new Object[][] { //
                { "A1", 123L }, //
                { "A2", 234L }, //
                { "A4", 999L }, //
        };
        uploadHdfsDataUnit(lastActivityDate, LAST_ACTIVITY_DATE_FIELDS);

        // AccountId, CDLUpdatedTime, CompanyName, CustomerAccountId
        Object[][] account = new Object[][] { //
                { "A1", 123L, "Company 1", "CA1" }, //
                { "A2", 115L, "Company 2", "CA2" }, //
                { "A3", 35L, "Company 3", "CA3" }, //
                { "A4", 10531L, "Company 4", "CA4" }, //
                { "A5", 1L, "Company 5", "CA5" }, //
        };
        uploadHdfsDataUnit(account, ACC_BATCH_STORE_FIELDS);

        // AccountId, NumberOfContacts
        Object[][] numOfContacts = new Object[][] { //
                { "A1", 0L }, //
                { "A2", 155L }, //
                { "A3", 100000L }, //
                { "A4", 3L }, //
                { "A5", 51L }, //
        };
        uploadHdfsDataUnit(numOfContacts, NUM_CONTACT_FIELDS);
    }

    private MergeCuratedAttributesConfig baseConfig() {
        MergeCuratedAttributesConfig config = new MergeCuratedAttributesConfig();
        config.joinKey = AccountId.name();
        config.lastActivityDateInputIdx = 0;
        config.masterTableIdx = 1;
        config.attrsToMerge.put(1, Collections.singletonMap(CDLUpdatedTime.name(), EntityLastUpdatedDate.name()));
        config.attrsToMerge.put(2, Collections.singletonMap(NumberOfContacts.name(), NumberOfContacts.name()));
        return config;
    }
}
