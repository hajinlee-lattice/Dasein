package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLCreatedTemplate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLUpdatedTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedSource;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedSystemType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityLastUpdatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfContacts;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateCuratedAttributesTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateCuratedAttributesTestNG.class);

    private static final String TEMPLATE_UPDATE_TIME_FIELD = String.format("Task_ABC__%s", CDLUpdatedTime.name());
    private static final String ENTITY_ATTR_PREFIX = "Account__";

    // input schema
    private static final List<Pair<String, Class<?>>> ACC_BATCH_STORE_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CDLCreatedTemplate.name(), String.class), //
            Pair.of(CDLUpdatedTime.name(), Long.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(CustomerAccountId.name(), String.class) //
    );

    private static final List<Pair<String, Class<?>>> CT_BATCH_STORE_FIELDS = Arrays.asList( //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(CDLCreatedTemplate.name(), String.class), //
            Pair.of(CDLUpdatedTime.name(), Long.class), //
            Pair.of(CompanyName.name(), String.class), //
            Pair.of(CustomerAccountId.name(), String.class) //
    );

    private static final List<Pair<String, Class<?>>> LAST_ACTIVITY_DATE_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class) //
    );

    private static final List<Pair<String, Class<?>>> CT_LAST_ACTIVITY_DATE_FIELDS = Arrays.asList( //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class), //
            Pair.of(AccountId.name(), String.class) //
    );

    private static final List<Pair<String, Class<?>>> NUM_CONTACT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(NumberOfContacts.name(), Long.class) //
    );
    private static final List<Pair<String, Class<?>>> SYSTEM_STORE_FIELDS = Arrays.asList( //
            Pair.of(EntityId.name(), String.class), //
            Pair.of(TEMPLATE_UPDATE_TIME_FIELD, Long.class) //
    );

    private static final List<Pair<String, Class<?>>> OUTPUT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ENTITY_ATTR_PREFIX + EntityCreatedSource.name(), String.class), //
            Pair.of(ENTITY_ATTR_PREFIX + EntityCreatedType.name(), String.class), //
            Pair.of(EntityCreatedSystemType.name(), String.class), //
            Pair.of(LastActivityDate.name(), Long.class), //
            Pair.of(EntityLastUpdatedDate.name(), Long.class), //
            Pair.of(TEMPLATE_UPDATE_TIME_FIELD.replace(CDLUpdatedTime.name(), EntityLastUpdatedDate.name()),
                    Long.class), //
            Pair.of(NumberOfContacts.name(), Long.class) //
    );

    // TODO add other test cases
    @Test(groups = "functional")
    private void testAccounts() {
        GenerateCuratedAttributesConfig config = baseConfig();
        prepareTestData();
        log.info("Config = {}", JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateCuratedAttributes.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Test(groups = "functional")
    private void testContacts() {
        GenerateCuratedAttributesConfig config = baseContactConfig();
        prepareContactTestData();
        log.info("Config = {}", JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateCuratedAttributes.class, config);
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

    @Override
    protected List<String> getInputOrder() {
        return getInputUnits() //
                .keySet() //
                .stream() //
                .sorted(Comparator.comparing(Function.identity())) //
                .collect(Collectors.toList());
    }

    private void prepareTestData() {
        // AccountId, LastActivityDate
        Object[][] lastActivityDate = new Object[][] { //
                { "A1", 123L }, //
                { "A2", 234L }, //
                { "A4", 999L }, //
        };
        uploadHdfsDataUnit(lastActivityDate, LAST_ACTIVITY_DATE_FIELDS);

        // AccountId, CDLCreatedTemplate, CDLUpdatedTime, CompanyName, CustomerAccountId
        Object[][] account = new Object[][] { //
                { "A1", "tmpl1", 123L, "Company 1", "CA1" }, //
                { "A2", "tmpl2", 115L, "Company 2", "CA2" }, //
                { "A3", "tmpl1", 35L, "Company 3", "CA3" }, //
                { "A4", "tmpl1", 10531L, "Company 4", "CA4" }, //
                { "A5", "tmpl3", 1L, "Company 5", "CA5" }, //
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

        // EntityId, last update time for template
        Object[][] accountSystemStore = new Object[][] { //
                { "A1", 163L }, //
                { "A6", 999L }, //
        };
        uploadHdfsDataUnit(accountSystemStore, SYSTEM_STORE_FIELDS);
    }

    private void prepareContactTestData() {
        // ContactId, LastActivityDate, AccountId
        Object[][] lastActivityDate = new Object[][] { //
                { "C1", 123L, "A1" }, //
                { "C2", 234L, "A2" }, //
                { "C4", 999L, "A3" }, //
        };
        uploadHdfsDataUnit(lastActivityDate, CT_LAST_ACTIVITY_DATE_FIELDS);

        // ContactId, AccountId, CDLCreatedTemplate, CDLUpdatedTime, CompanyName,
        // CustomerContactId
        Object[][] contact = new Object[][] { //
                { "C1", "A1", "tmpl1", 123L, "Company 1", "CC1" }, //
                { "C2", "A2", "tmpl2", 115L, "Company 2", "CC2" }, //
                { "C3", "A3", "tmpl1", 35L, "Company 3", "CC3" }, //
                { "C4", "A4", "tmpl1", 10531L, "Company 4", "CC4" }, // orphan
                { "C5", "A5", "tmpl3", 1L, "Company 5", "CC5" }, // orphan
        };
        uploadHdfsDataUnit(contact, CT_BATCH_STORE_FIELDS);

        // AccountId, CDLCreatedTemplate, CDLUpdatedTime, CompanyName, CustomerAccountId
        Object[][] account = new Object[][] { //
                { "A1", "tmpl1", 123L, "Company 1", "CA1" }, //
                { "A2", "tmpl2", 115L, "Company 2", "CA2" }, //
                { "A3", "tmpl1", 35L, "Company 3", "CA3" }, //
        };
        uploadHdfsDataUnit(account, ACC_BATCH_STORE_FIELDS);
    }

    private GenerateCuratedAttributesConfig baseConfig() {
        GenerateCuratedAttributesConfig config = new GenerateCuratedAttributesConfig();
        config.joinKey = AccountId.name();
        config.lastActivityDateInputIdx = 0;
        config.masterTableIdx = 1;
        Map<String, String> templateSystemMap = new HashMap<>();
        templateSystemMap.put("tmpl1", "System 1");
        templateSystemMap.put("tmpl2", "System 2");
        Map<String, String> templateEntityTypeMap = new HashMap<>();
        templateEntityTypeMap.put("tmpl1", "Account");
        templateEntityTypeMap.put("tmpl2", "Contact");
        Map<String, String> templateSystemTypeMap = new HashMap<>();
        templateSystemTypeMap.put("tmpl1", "Pardot");
        templateSystemTypeMap.put("tmpl2", "Marketo");
        Map<String, String> accBatchStoreAttrs = new HashMap<>();
        accBatchStoreAttrs.put(CDLUpdatedTime.name(), EntityLastUpdatedDate.name());
        accBatchStoreAttrs.put(CDLCreatedTemplate.name(), ENTITY_ATTR_PREFIX);
        config.attrsToMerge.put(1, accBatchStoreAttrs);
        config.attrsToMerge.put(2, Collections.singletonMap(NumberOfContacts.name(), NumberOfContacts.name()));
        config.attrsToMerge.put(3, Collections.singletonMap(TEMPLATE_UPDATE_TIME_FIELD,
                TEMPLATE_UPDATE_TIME_FIELD.replace(CDLUpdatedTime.name(), EntityLastUpdatedDate.name())));
        config.joinKeys.put(3, EntityId.name());
        config.templateSystemMap.putAll(templateSystemMap);
        config.templateTypeMap.putAll(templateEntityTypeMap);
        config.templateSystemTypeMap.putAll(templateSystemTypeMap);
        return config;
    }

    private GenerateCuratedAttributesConfig baseContactConfig() {
        GenerateCuratedAttributesConfig config = new GenerateCuratedAttributesConfig();
        config.joinKey = ContactId.name();
        config.columnsToIncludeFromMaster = Collections.singletonList(AccountId.name());
        config.lastActivityDateInputIdx = 4;
        config.masterTableIdx = 5;
        config.parentMasterTableIdx = 6;
        config.joinKeys.put(6, AccountId.name());

        return config;
    }
}
