package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ActivityType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__Row_Count__;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CalculateLastActivityDateConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalculateLastActivityDateTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CalculateLastActivityDateTestNG.class);

    // input schema
    private static final List<Pair<String, Class<?>>> OPP_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LastModifiedDate.name(), Long.class), //
            Pair.of(StageName.name(), String.class), //
            Pair.of(CustomerAccountId.name(), String.class) //
    );
    private static final List<Pair<String, Class<?>>> OPP_PERIOD_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class), //
            Pair.of(StageName.name(), String.class), //
            Pair.of(__Row_Count__.name(), Long.class) //
    );
    private static final List<Pair<String, Class<?>>> CTK_STREAM_IMPORT_FIELDS = Arrays.asList( //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class), //
            Pair.of(ActivityType.name(), String.class), //
            Pair.of(__Row_Count__.name(), Long.class) //
    );
    private static final List<Pair<String, Class<?>>> CTK_TABLE_FIELDS = Arrays.asList( //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ContactName.name(), String.class), //
            Pair.of(PhoneNumber.name(), String.class) //
    );

    // output schema
    private static final List<String> ACCOUNT_LAST_ACTIVITY_DATE_FIELDS = Arrays.asList(AccountId.name(), LastActivityDate.name());
    private static final List<String> CONTACT_LAST_ACTIVITY_DATE_FIELDS = Arrays.asList(ContactId.name(), LastActivityDate.name());

    // TODO add other test cases
    @Test(groups = "functional")
    private void test() {
        CalculateLastActivityDateConfig config = baseConfig();
        prepareTestData();
        log.info("Config = {}", JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateLastActivityDate.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(verifyEntity(ACCOUNT_LAST_ACTIVITY_DATE_FIELDS), verifyEntity(CONTACT_LAST_ACTIVITY_DATE_FIELDS));
    }

    private Function<HdfsDataUnit, Boolean> verifyEntity(List<String> columns) {
        return (tgt) -> {
            AtomicInteger counter = new AtomicInteger(0);
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                counter.incrementAndGet();
                log.info(debugStr(record, columns));
            });
            log.info("Number of records = {}", counter.get());
            return true;
        };
    }

    private void prepareTestData() {
        prepareAccountStream();
        prepareContactStream();
    }

    private void prepareContactStream() {
        // ContactId, AccountId, LastActivityDate, ActivityType, __Row_Count__
        // AccountId is set to junk values intentionally to test contact batch store
        Object[][] importData = new Object[][] { //
                { "C1", "sldkfjkls", 99999L, "Email Sent", 1L }, // last activity date for a1, c1
                { "C1", "dfdfdfd", 125L, "Email Sent", 1L }, //
                { "C1", "dfksjld", 100L, "Email Sent", 2L }, //
                { "C1", "dfksjld", 99L, "Form Filled", 500L }, //
                { "C2", "dfksjld", 1000L, "Form Submitted", 3L }, // last activity date for c2
                { "C2", "dfksjld", 999L, "Email Clicked", 1005L }, //
                { "C2", "dfksjld", 345L, "Form Filled", 9876L }, //
                { "C3", "dfksjld", 1L, "Email Sent", 8316L }, //
                { "C3", "dfksjld", 5L, "Form Filled", 15L }, // last activity date for c3
                { "C4", "dfksjld", 9999L, "Email Sent", 18L }, // last activity date for a3, c4
        };
        uploadHdfsDataUnit(importData, CTK_STREAM_IMPORT_FIELDS);

        // ContactId, AccountId, ContactName, PhoneNumber
        Object[][] ctkBatchStore = new Object[][] { //
                { "C1", "A1", "john doe", "(000)-000-0000" }, //
                { "C2", "A1", "jane doe", "(000)-000-0000" }, //
                { "C3", "A2", "tourist", "(000)-000-0000" }, //
                { "C4", "A3", "hello world", "(000)-000-0000" }, //
        };
        uploadHdfsDataUnit(ctkBatchStore, CTK_TABLE_FIELDS);
    }

    private void prepareAccountStream() {
        // AccountId, LastModifiedDate, StageName, CustomerAccountId
        Object[][] importData = new Object[][] { //
                { "A1", 123L, "Stage1", "acc_01" }, //
                { "A1", 125L, "Stage2", "acc_01" }, //
                { "A1", 100L, "Stage2", "acc_01" }, //
                { "A1", 99L, "Stage2", "acc_01" }, //
                { "A2", 0L, "Stage2", "acc_02" }, //
                { "A2", 999L, "Stage4", "acc_02" }, //
                { "A2", 15321L, "Stage1", "acc_02" }, // last activity date for a2
                { "A3", 1L, "Stage4", "acc_03" }, //
                { "A3", 1L, "Stage1", "acc_03" }, //
                { "A4", 9877L, "Stage9", "acc_04" }, // last activity date for a4
        };
        uploadHdfsDataUnit(importData, OPP_IMPORT_FIELDS);

        // AccountId, LastActivityDate, StageName, __Row_Count__
        Object[][] periodStoreData = new Object[][] { //
                { "A1", 12345L, "Stage1", 3L }, //
                { "A1", 99L, "Stage2", 1L }, //
                { "A2", 15320L, "Stage2", 100L }, //
                { "A2", 15321L, "Stage4", 9991L }, // last activity date for a2
                { "A2", 15321L, "Stage1", 12345L }, // last activity date for a2
                { "A4", 9876L, "Stage9", 4L }, //
        };
        uploadHdfsDataUnit(periodStoreData, OPP_PERIOD_FIELDS);
    }

    private CalculateLastActivityDateConfig baseConfig() {
        CalculateLastActivityDateConfig config = new CalculateLastActivityDateConfig();
        config.accountStreamInputIndices.add(0);
        config.accountStreamInputIndices.add(1);
        config.contactStreamInputIndices.add(2);
        config.contactTableIdx = 3;
        config.dateAttrs = Arrays.asList(LastModifiedDate.name(), LastActivityDate.name(), LastActivityDate.name(), null);
        return config;
    }
}
