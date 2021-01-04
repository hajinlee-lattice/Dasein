package com.latticeengines.eai.service.impl.dynamodb;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_EXPORT_TYPE;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_TABLE_NAME;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.aws.dynamo.impl.DynamoItemServiceImpl;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.dynamodb.runtime.AccountLookupToDynamoJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;

/**
 * This test requires a local Dynamo running at port 8000
 */
public class AccountLookupToDynamoMapperTestNG extends EaiMiniClusterFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AccountLookupToDynamoMapperTestNG.class);

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("DynamoTestCustomer");
    private static final String AtlasLookupKey = InterfaceName.AtlasLookupKey.name();
    private static final String DYNAMO_VALUE_FIELD = "AccountId";
    private static final String ATTR_ACCOUNT_ID = "AccountId";
    private static final String ATTR_LOOKUP_ID_1 = "SalesforceAccount1";
    private static final String ATTR_LOOKUP_ID_2 = "SalesforceAccount2";
    private static final String KEY_FORMAT = "AccountLookup__%s__%s__%s_%s"; // tenantId__ver__lookupId_lookIdVal
    private static final String EXPORT_TYPE = "AccountLookupToDynamoStepConfiguration";
    private static final int SWITCH_IDX = 3;
    private static Table accountLookupTable;

    @Inject
    private DynamoService dynamoService;

    private DynamoItemService dynamoItemService;

    @Resource(name = "dynamoExportService")
    private ExportService exportService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.dynamo.endpoint}")
    private String endpoint;

    @Value("${aws.default.access.key}")
    private String accessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String secretKey;

    private final String sourceDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/input";
    private final String sourceFilePath = sourceDir + "/accountLookup.avro";
    private final String targetDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/output";
    private String dynamoTableName;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        super.setup();
        dynamoTableName = String.format("%s_%s_AccountLookupToDynamoTest", leEnv, leStack);

        HdfsUtils.rmdir(miniclusterConfiguration, sourceDir);
        HdfsUtils.rmdir(miniclusterConfiguration, targetDir);
        HdfsUtils.mkdir(miniclusterConfiguration, sourceDir);

        dynamoService.switchToLocal(true);
        dynamoService.deleteTable(dynamoTableName);

        dynamoService.createTable(dynamoTableName, 10, 10, //
                AtlasLookupKey, ScalarAttributeType.S.name(), null, null, null);
        dynamoItemService = new DynamoItemServiceImpl(dynamoService);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());

        accountLookupTable = setupAccountLookupTable();
    }

    @AfterClass(groups = "dynamo")
    public void cleanup() throws IOException {
        dynamoService.deleteTable(dynamoTableName);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());
        super.clear();
    }

    @Test(groups = "dynamo")
    public void testInitPublish() throws Exception {
        int targetVersion = 0;
        submitPublish(Integer.toString(targetVersion));
        verifyVersion(targetVersion);
    }

    @Test(groups = "dynamo", dependsOnMethods = "testInitPublish")
    public void testRefresh() throws Exception {
        int targetVersion = 1;
        submitPublish(Integer.toString(targetVersion));
        verifyVersion(targetVersion);
    }

    @Test(groups = "manual")
    public void createLocalTable() {
        dynamoService.createTable("AccountLookup_20201010", 10, 10, //
                AtlasLookupKey, ScalarAttributeType.S.name(), null, null, null);
    }

    private void submitPublish(String version) throws Exception {
        HdfsToDynamoConfiguration eaiConfig = new HdfsToDynamoConfiguration();

        eaiConfig.setName("AccountLookupToDynamoTest");
        eaiConfig.setCustomerSpace(TEST_CUSTOMER);
        eaiConfig.setExportDestination(ExportDestination.DYNAMO);
        eaiConfig.setExportFormat(ExportFormat.AVRO);
        eaiConfig.setExportInputPath(accountLookupTable.getExtracts().get(0).getPath());
        eaiConfig.setUsingDisplayName(false);
        eaiConfig.setExportTargetPath("/tmp/path");

        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.NUM_MAPPERS, "2");

        props.put(CONFIG_TABLE_NAME, dynamoTableName);
        props.put(CONFIG_ATLAS_LOOKUP_IDS, ATTR_LOOKUP_ID_1 + "," + ATTR_LOOKUP_ID_2);
        props.put(CONFIG_ENDPOINT, endpoint);
        props.put(CONFIG_EXPORT_VERSION, version);
        props.put(CONFIG_EXPORT_TYPE, EXPORT_TYPE);
        long expTime = Instant.now().plus(30, ChronoUnit.DAYS).getEpochSecond();
        props.put(CONFIG_ATLAS_LOOKUP_TTL, Long.toString(expTime));

        eaiConfig.setProperties(props);

        Properties properties = ((DynamoExportServiceImpl) exportService).constructProperties(eaiConfig,
                new ExportContext(miniclusterConfiguration));
        properties.forEach((k, v) -> log.info("{}: {}", k, v));
        testMRJob(AccountLookupToDynamoJob.class, properties);
    }

    private void verifyVersion(int version) {
        for (int i = 0; i < 5; i++) {
            String key = String.format(KEY_FORMAT, TEST_CUSTOMER.getTenantId(), version,
                    i < SWITCH_IDX ? ATTR_LOOKUP_ID_1 : ATTR_LOOKUP_ID_2,
                    i < SWITCH_IDX ? toLookupIdVal1(i) : toLookupIdVal2(i));
            System.out.println("key:::" + key);
            Item item = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(AtlasLookupKey, key));
            Assert.assertNotNull(item);
            Assert.assertEquals(item.getString(ATTR_ACCOUNT_ID), toAccountId(i));
        }
    }

    private Table setupAccountLookupTable() throws IOException {
        Schema lookupSchema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"AccountLookup\",\"doc\":\"Testing data\"," //
                        + "\"fields\":[" //
                        + "{\"name\":\"" + ATTR_ACCOUNT_ID + "\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"" + AtlasLookupKey + "\",\"type\":[\"string\",\"null\"]}" //
                        + "]}");
        List<GenericRecord> recordList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            recordList.add(new GenericRecordBuilder(lookupSchema) //
                    .set(ATTR_ACCOUNT_ID, toAccountId(i)) //
                    .set(AtlasLookupKey, i < SWITCH_IDX ? toLookupId1(i) : toLookupId2(i)) //
                    .build() //
            );
        }
        AvroUtils.writeToHdfsFile(miniclusterConfiguration, lookupSchema, sourceFilePath, recordList);

        Table table = new Table();
        table.setName("AccountLookup");

        Attribute attr1 = new Attribute();
        attr1.setName(ATTR_ACCOUNT_ID);
        attr1.setDataType("String");
        table.addAttribute(attr1);

        Attribute attr2 = new Attribute();
        attr2.setName(AtlasLookupKey);
        attr2.setDataType("String");
        table.addAttribute(attr2);

        Extract extract = new Extract();
        extract.setPath(sourceFilePath);
        table.setExtracts(Collections.singletonList(extract));

        return table;
    }

    private String toAccountId(int idx) {
        return String.format("%09d", 100000 + idx);
    }

    private String toLookupId1(int idx) {
        return String.format("%s_%09d", ATTR_LOOKUP_ID_1, 200000 + idx);
    }

    private String toLookupIdVal1(int idx) {
        return String.format("%09d", 200000 + idx);
    }

    private String toLookupId2(int idx) {
        return String.format("%s_%09d", ATTR_LOOKUP_ID_2, 300000 + idx);
    }

    private String toLookupIdVal2(int idx) {
        return String.format("%09d", 300000 + idx);
    }
}
