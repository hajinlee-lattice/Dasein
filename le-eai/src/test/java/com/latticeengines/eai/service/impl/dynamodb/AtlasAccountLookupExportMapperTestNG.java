package com.latticeengines.eai.service.impl.dynamodb;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_EXPORT_TYPE;
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
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
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
import com.latticeengines.eai.dynamodb.runtime.AtlasAccountLookupExportJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.yarn.exposed.service.impl.JobServiceImpl;

/**
 * This test requires a local Dynamo running at port 8000
 */
public class AtlasAccountLookupExportMapperTestNG extends EaiMiniClusterFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AtlasAccountLookupExportMapperTestNG.class);

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("DynamoTestCustomer");
    private static final String DYNAMO_PK_FIELD = InterfaceName.AtlasLookupKey.name();
    private static final String DYNAMO_VALUE_FIELD = "AccountId";
    private static final String ATTR_ACCOUNT_ID = "AccountId";
    private static final String ATTR_LOOKUP_ID_1 = "SalesforceAccount1";
    private static final String ATTR_LOOKUP_ID_2 = "SalesforceAccount2";
    private static final String CUSTOMER_ACCOUNT_ID = InterfaceName.CustomerAccountId.name();
    private static final String ROW_ID = "RowId";
    private static final String COLUMN_ID = "ColumnId";
    private static final String FROM_STRING = "FromString";
    private static final String TO_STRING = "ToString";
    private static final String DELETED = "Deleted";
    private static final String KEY_FORMAT = "AccountLookup__%s__%s__%s_%s"; // tenantId__ver__lookupId_lookIdVal
    private static final String LOOKUP_CHANGED = "__Changed__";
    private static final String LOOKUP_DELETED = "__Deleted__";
    private static final String EXPORT_TYPE = "atlasAccountLookupExportStepConfiguration";

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
    private final String sourceFilePath = sourceDir + "/LatticeAccount.avro";
    private final String sourceFilePath2 = sourceDir + "/LatticeAccount2.avro";
    private final String targetDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/output";
    private final String currentVersion = "0";
    private String dynamoTableName;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.WARN);
        super.setup();
        dynamoTableName = String.format("%s_%s_AtlasAccountLookupTest", leEnv, leStack);

        HdfsUtils.rmdir(miniclusterConfiguration, sourceDir);
        HdfsUtils.rmdir(miniclusterConfiguration, targetDir);
        HdfsUtils.mkdir(miniclusterConfiguration, sourceDir);

        dynamoService.switchToLocal(true);
        dynamoService.deleteTable(dynamoTableName);
        dynamoService.createTable(dynamoTableName, 10, 10, //
                DYNAMO_PK_FIELD, ScalarAttributeType.S.name(), null, null, null);
        dynamoItemService = new DynamoItemServiceImpl(dynamoService);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());
    }

    @AfterClass(groups = "dynamo")
    public void cleanup() throws IOException {
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.INFO);
        dynamoService.deleteTable(dynamoTableName);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());
        super.clear();
    }

    @Test(groups = "dynamo")
    public void testInitLookupTable() throws Exception {
        submitExport(createInitChangeList(), sourceFilePath);
        String tenant = CustomerSpace.shortenCustomerSpace(TEST_CUSTOMER.toString());
        for (int i = 0; i < 5; i++) {
            String accountId = toAccountId(i);
            String key = String.format(KEY_FORMAT, tenant, currentVersion, ATTR_LOOKUP_ID_1, toLookupId1(i));
            Item item = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, key));
            Assert.assertNotNull(item);
            Assert.assertEquals(item.getString(DYNAMO_VALUE_FIELD), accountId);
        }
        for (int i = 0; i < 5; i++) {
            String accountId = toAccountId(i);
            String key = String.format(KEY_FORMAT, tenant, currentVersion, CUSTOMER_ACCOUNT_ID, toCustomerAccountId(i));
            Item item = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, key));
            Assert.assertNotNull(item);
            Assert.assertEquals(item.getString(DYNAMO_VALUE_FIELD), accountId);
        }
    }

    @Test(groups = "dynamo", dependsOnMethods = "testInitLookupTable")
    public void testPopulateChangelist() throws Exception {
        // changed lookup for account 2 from 2 to 200
        // remove lookup for account 3
        submitExport(createUpdateChangeList(), sourceFilePath2);

        String tenant = CustomerSpace.shortenCustomerSpace(TEST_CUSTOMER.toString());

        // verify account 1 customerAccountId changed
        String acc1OldKey = String.format(KEY_FORMAT, tenant, currentVersion, CUSTOMER_ACCOUNT_ID, toCustomerAccountId(1));
        String acc1NewKey = String.format(KEY_FORMAT, tenant, currentVersion, CUSTOMER_ACCOUNT_ID, toCustomerAccountId(100));
        Item acc1OldItem = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, acc1OldKey));
        Item acc1NewItem = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, acc1NewKey));
        Assert.assertTrue(acc1OldItem.getBoolean(LOOKUP_DELETED));
        Assert.assertTrue(acc1NewItem.getBoolean(LOOKUP_CHANGED));

        // verify account 2 old record marked as deleted
        // new record marked as changed
        String acc2OldKey = String.format(KEY_FORMAT, tenant, currentVersion, ATTR_LOOKUP_ID_1, toLookupId1(2));
        String acc2NewKey = String.format(KEY_FORMAT, tenant, currentVersion, ATTR_LOOKUP_ID_1, toLookupId1(200));
        Item acc2OldItem = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, acc2OldKey));
        Item acc2NewItem = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, acc2NewKey));
        Assert.assertTrue(acc2OldItem.getBoolean(LOOKUP_DELETED));
        Assert.assertTrue(acc2NewItem.getBoolean(LOOKUP_CHANGED));

        // verify account 3 lookup item marked deleted
        String acc3Key = String.format(KEY_FORMAT, tenant, currentVersion, ATTR_LOOKUP_ID_1, toLookupId1(3));
        Item acc3Item = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, acc3Key));
        Assert.assertTrue(acc3Item.getBoolean(LOOKUP_DELETED));
    }

    private void submitExport(Table table, String extractPath) throws Exception {
        HdfsToDynamoConfiguration fileExportConfig = new HdfsToDynamoConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.AVRO);
        fileExportConfig.setExportDestination(ExportDestination.DYNAMO);
        fileExportConfig.setCustomerSpace(TEST_CUSTOMER);

        Extract extract = new Extract();
        extract.setPath(extractPath);
        table.setExtracts(Collections.singletonList(extract));
        fileExportConfig.setExportInputPath(sourceDir);

        fileExportConfig.setTable(table);
        fileExportConfig.setExportTargetPath(targetDir);
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.NUM_MAPPERS, "2");

        props.put(CONFIG_TABLE_NAME, dynamoTableName);
        props.put(CONFIG_ATLAS_LOOKUP_IDS, String.join(",", ATTR_LOOKUP_ID_1, ATTR_LOOKUP_ID_2));
        props.put(CONFIG_ENDPOINT, endpoint);
        props.put(CONFIG_EXPORT_VERSION, currentVersion);
        props.put(CONFIG_EXPORT_TYPE, EXPORT_TYPE);
        long expTime = Instant.now().plus(30, ChronoUnit.DAYS).getEpochSecond();
        props.put(CONFIG_ATLAS_LOOKUP_TTL, Long.toString(expTime));

        fileExportConfig.setProperties(props);

        Properties properties = ((DynamoExportServiceImpl) exportService).constructProperties(fileExportConfig,
                new ExportContext(miniclusterConfiguration));
        properties.forEach((k, v) -> log.info("{}: {}", k, v));
        testMRJob(AtlasAccountLookupExportJob.class, properties);
    }

    private Table createInitChangeList() throws IOException {
        Table table = new Table();
        table.setName("Account_ChangeList");
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Account\",\"doc\":\"Testing data\"," //
                + "\"fields\":[" //
                + "{\"name\":\"" + ROW_ID + "\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"" + COLUMN_ID + "\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"" + DELETED + "\",\"type\":[\"boolean\",\"null\"]}," //
                + "{\"name\":\"" + FROM_STRING + "\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"" + TO_STRING + "\",\"type\":[\"string\",\"null\"]}" //
                + "]}");
        List<GenericRecord> recordList = new ArrayList<>();

        // creating new lookup record for accounts 1 to 5
        for (int i = 0; i < 5; i++) {
            recordList.add(new GenericRecordBuilder(schema).set(ROW_ID, toAccountId(i)) //
                    .set(COLUMN_ID, ATTR_LOOKUP_ID_1) //
                    .set(DELETED, null) //
                    .set(FROM_STRING, null) //
                    .set(TO_STRING, toLookupId1(i)) //
                    .build() //
            );
        }
        for (int i = 0; i < 5; i++) {
            recordList.add(new GenericRecordBuilder(schema).set(ROW_ID, toAccountId(i)) //
                    .set(COLUMN_ID, CUSTOMER_ACCOUNT_ID) //
                    .set(DELETED, null) //
                    .set(FROM_STRING, null) //
                    .set(TO_STRING, toCustomerAccountId(i)) //
                    .build() //
            );
        }

        AvroUtils.writeToHdfsFile(miniclusterConfiguration, schema, sourceFilePath, recordList);

        Attribute attr1 = new Attribute();
        attr1.setName(ROW_ID);
        attr1.setDataType("String");
        table.addAttribute(attr1);

        Attribute attr2 = new Attribute();
        attr2.setName(COLUMN_ID);
        attr2.setDataType("String");
        table.addAttribute(attr2);

        Attribute attr3 = new Attribute();
        attr3.setName(FROM_STRING);
        attr3.setDataType("String");
        table.addAttribute(attr3);

        Attribute attr4 = new Attribute();
        attr4.setName(TO_STRING);
        attr4.setDataType("String");
        table.addAttribute(attr4);

        Attribute attr5 = new Attribute();
        attr5.setName(DELETED);
        attr5.setDataType("Boolean");
        table.addAttribute(attr5);

        return table;
    }

    private Table createUpdateChangeList() throws IOException {
        Table table = new Table();
        table.setName("Account_ChangeList");
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Account\",\"doc\":\"Testing data\"," //
                + "\"fields\":[" //
                + "{\"name\":\"" + ROW_ID + "\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"" + COLUMN_ID + "\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"" + DELETED + "\",\"type\":[\"boolean\",\"null\"]}," //
                + "{\"name\":\"" + FROM_STRING + "\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"" + TO_STRING + "\",\"type\":[\"string\",\"null\"]}" //
                + "]}");
        List<GenericRecord> recordList = new ArrayList<>();
        // update customerAccountId for account 1
        recordList.add(new GenericRecordBuilder(schema).set(ROW_ID, toAccountId(1)) //
                .set(COLUMN_ID, CUSTOMER_ACCOUNT_ID) //
                .set(DELETED, true) //
                .set(FROM_STRING, toCustomerAccountId(1)) //
                .set(TO_STRING, toCustomerAccountId(100)) //
                .build() //
        );
        // update lookup Id value
        recordList.add(new GenericRecordBuilder(schema).set(ROW_ID, toAccountId(2)) //
                .set(COLUMN_ID, ATTR_LOOKUP_ID_1) //
                .set(DELETED, null) //
                .set(FROM_STRING, toLookupId1(2)) //
                .set(TO_STRING, toLookupId1(200).toUpperCase()) // should be converted to lowercase by mapper
                .build() //
        );
        // delete lookup Id value for account 3
        recordList.add(new GenericRecordBuilder(schema).set(ROW_ID, toAccountId(3)) //
                .set(COLUMN_ID, ATTR_LOOKUP_ID_1) //
                .set(DELETED, true) //
                .set(FROM_STRING, toLookupId1(3)) //
                .set(TO_STRING, null) //
                .build() //
        );
        AvroUtils.writeToHdfsFile(miniclusterConfiguration, schema, sourceFilePath2, recordList);

        Attribute attr1 = new Attribute();
        attr1.setName(ROW_ID);
        attr1.setDataType("String");
        table.addAttribute(attr1);

        Attribute attr2 = new Attribute();
        attr2.setName(COLUMN_ID);
        attr2.setDataType("String");
        table.addAttribute(attr2);

        Attribute attr3 = new Attribute();
        attr3.setName(FROM_STRING);
        attr3.setDataType("String");
        table.addAttribute(attr3);

        Attribute attr4 = new Attribute();
        attr4.setName(TO_STRING);
        attr4.setDataType("String");
        table.addAttribute(attr4);

        Attribute attr5 = new Attribute();
        attr5.setName(DELETED);
        attr5.setDataType("Boolean");
        table.addAttribute(attr5);

        return table;
    }

    private String toAccountId(int idx) {
        return String.format("%09d", 100000 + idx);
    }

    private String toLookupId1(int idx) {
        return String.format("aaa%09d", 200000 + idx);
    }

    private String toLookupId2(int idx) {
        return String.format("bbb%09d", 300000 + idx);
    }

    private String toCustomerAccountId(int idx) {
        return "ca_" + idx;
    }

}
