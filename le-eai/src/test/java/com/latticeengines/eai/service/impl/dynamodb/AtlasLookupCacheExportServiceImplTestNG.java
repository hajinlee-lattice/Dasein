package com.latticeengines.eai.service.impl.dynamodb;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
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
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.dynamodb.runtime.AtlasLookupCacheExportJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.yarn.exposed.service.impl.JobServiceImpl;

public class AtlasLookupCacheExportServiceImplTestNG extends EaiMiniClusterFunctionalTestNGBase {

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("DynamoTestCustomer");
    private static final String DYNAMO_PK_FIELD = "Key";
    private static final String DYNAMO_VALUE_FIELD = "AccountId";
    private static final String ATTR_ACCOUNT_ID = "AccountId";
    private static final String ATTR_LOOKUP_ID_1 = "SalesforceAccount1";
    private static final String ATTR_LOOKUP_ID_2 = "SalesforceAccount2";

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

    private String sourceDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/input";
    private String sourceFilePath = sourceDir + "/LatticeAccount.avro";
    private String targetDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/output";
    private Table table;
    private String dynamoTableName;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.WARN);
        super.setup();
    }

    @AfterClass(groups = "dynamo")
    public void cleanup() throws IOException {
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.INFO);
        super.clear();
    }

    @Test(groups = "dynamo")
    public void exportAccountLookup() throws Exception {
        dynamoTableName = String.format("%s_%s_AtlasLookupCacheTest", leEnv, leStack);

        HdfsUtils.rmdir(miniclusterConfiguration, sourceDir);
        HdfsUtils.rmdir(miniclusterConfiguration, targetDir);
        HdfsUtils.mkdir(miniclusterConfiguration, sourceDir);

        dynamoService.switchToLocal(true);
        dynamoService.deleteTable(dynamoTableName);
        dynamoService.createTable(dynamoTableName, 10, 10, //
                DYNAMO_PK_FIELD, ScalarAttributeType.S.name(), null, null);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());

        table = createAccountTable();
        submitExport();

        dynamoItemService = new DynamoItemServiceImpl(dynamoService);
        String tenant = CustomerSpace.shortenCustomerSpace(TEST_CUSTOMER.toString());
        for (int i = 0; i < 10; i++) {
            int idx = new Random().nextInt(10000);
            String accountId = toAccountId(idx);

            String key1 = String.format("%s_%s_%s", tenant, ATTR_LOOKUP_ID_1, toLookupId1(idx));
            Item item = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, key1));
            Assert.assertNotNull(item);
            Assert.assertEquals(item.getString(DYNAMO_VALUE_FIELD), accountId);

            String key2 = String.format("%s_%s_%s", tenant, ATTR_LOOKUP_ID_2, toLookupId2(idx));
            item = dynamoItemService.getItem(dynamoTableName, new PrimaryKey(DYNAMO_PK_FIELD, key2));
            Assert.assertNotNull(item);
            Assert.assertEquals(item.getString(DYNAMO_VALUE_FIELD), accountId);
        }

        dynamoService.deleteTable(dynamoTableName);
        result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());
    }

    private void submitExport() throws Exception {
        HdfsToDynamoConfiguration fileExportConfig = new HdfsToDynamoConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.AVRO);
        fileExportConfig.setExportDestination(ExportDestination.DYNAMO);
        fileExportConfig.setCustomerSpace(TEST_CUSTOMER);

        Extract extract = new Extract();
        extract.setPath(sourceFilePath);
        table.setExtracts(Collections.singletonList(extract));
        fileExportConfig.setExportInputPath(sourceDir);

        fileExportConfig.setTable(table);
        fileExportConfig.setExportTargetPath(targetDir);
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.NUM_MAPPERS, "2");

        props.put(CONFIG_TABLE_NAME, dynamoTableName);
        props.put(CONFIG_ATLAS_LOOKUP_IDS, ATTR_LOOKUP_ID_1 + "," + ATTR_LOOKUP_ID_2);
        props.put(CONFIG_ENDPOINT, endpoint);

        fileExportConfig.setProperties(props);

        Properties properties = ((DynamoExportServiceImpl) exportService).constructProperties(fileExportConfig,
                new ExportContext(miniclusterConfiguration));
        testMRJob(AtlasLookupCacheExportJob.class, properties);
    }

    private Table createAccountTable() throws IOException {
        Table table = new Table();
        table.setName("Account");
        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Account\",\"doc\":\"Testing data\"," //
                        + "\"fields\":[" //
                        + "{\"name\":\"" + ATTR_ACCOUNT_ID + "\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"" + ATTR_LOOKUP_ID_1 + "\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"" + ATTR_LOOKUP_ID_2 + "\",\"type\":[\"string\",\"null\"]}" //
                        + "]}");
        List<GenericRecord> recordList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set(ATTR_ACCOUNT_ID, toAccountId(i));
            builder.set(ATTR_LOOKUP_ID_1, toLookupId1(i));
            builder.set(ATTR_LOOKUP_ID_2, toLookupId2(i));
            recordList.add(builder.build());
        }
        AvroUtils.writeToHdfsFile(miniclusterConfiguration, schema, sourceFilePath, recordList);

        Attribute attr1 = new Attribute();
        attr1.setName(ATTR_ACCOUNT_ID);
        attr1.setDataType("String");
        table.addAttribute(attr1);

        Attribute attr2 = new Attribute();
        attr2.setName(ATTR_LOOKUP_ID_1);
        attr2.setDataType("String");
        table.addAttribute(attr2);

        Attribute attr3 = new Attribute();
        attr3.setName(ATTR_LOOKUP_ID_2);
        attr3.setDataType("String");
        table.addAttribute(attr3);

        return table;
    }

    private String toAccountId(int idx) {
        return String.format("%09d", 100000 + idx);
    }

    private String toLookupId1(int idx) {
        return String.format("%09d", 200000 + idx);
    }

    private String toLookupId2(int idx) {
        return String.format("%09d", 300000 + idx);
    }


}
