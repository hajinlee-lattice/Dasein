package com.latticeengines.eai.service.impl.dynamodb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.dynamodb.runtime.DynamoExportJob;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.yarn.exposed.service.impl.JobServiceImpl;

public class DynamoExportServiceImplTestNG extends EaiFunctionalTestNGBase {

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("DynamoTestCustomer");
    private AmazonDynamoDBClient client;

    private static final String LATTICE_ACCOUNT = LatticeAccount.class.getSimpleName();
    private static final String ACCOUNT_LOOKUP_ENTRY = AccountLookupEntry.class.getSimpleName();

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DynamoService dynamoService;

    @Autowired
    @Qualifier("dynamoExportService")
    private ExportService exportService;

    @Autowired
    private FabricDataService fabricDataService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.dynamo.endpoint}")
    private String endpoint;

    @Value("${aws.default.access.key.encrypted}")
    private String accessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String secretKey;

    private String sourceDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/input";
    private String sourceFilePath = sourceDir + "/LatticeAccount.avro";
    private String targetDir = "/tmp/" + TEST_CUSTOMER.getContractId() + "/output";
    private Table table;
    private String repo;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.WARN);
    }

    @AfterClass(groups = "aws")
    public void cleanup() throws IOException {
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.INFO);
    }

    @Test(groups = "aws")
    public void exportLatticeAccount() throws Exception {
        setupMethod(LATTICE_ACCOUNT);

        table = createLatticeAccountTable();
        ExportContext ctx = submitExport(LatticeAccount.class, LATTICE_ACCOUNT);
        ApplicationId appId = ctx.getProperty(ExportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        TestLatticeAccountEntityMgrImpl entityMgr = new TestLatticeAccountEntityMgrImpl();
        entityMgr.init();

        for (int i = 0; i < 10; i++) {
            Integer idx = new Random().nextInt(10000);
            LatticeAccount account = entityMgr.findByKey(idx.toString());
            Assert.assertNotNull(account);
            System.out.println(JsonUtils.serialize(account));
        }
    }

    @Test(groups = "aws")
    public void exportAccountLookup() throws Exception {
        setupMethod(ACCOUNT_LOOKUP_ENTRY);

        table = createAccountLookupTable();
        ExportContext ctx = submitExport(AccountLookupEntry.class, ACCOUNT_LOOKUP_ENTRY);

        ApplicationId appId = ctx.getProperty(ExportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        TestAccountLookupEntityMgrImpl entityMgr = new TestAccountLookupEntityMgrImpl();
        entityMgr.init();

        for (int i = 0; i < 10; i++) {
            Integer idx = new Random().nextInt(10000);
            String key = String.format("_DOMAIN_lattice-engines.com_DUNS_%09d", idx);
            AccountLookupEntry account = entityMgr.findByKey(key);
            Assert.assertNotNull(account);
            Assert.assertEquals(account.getDomain(), "lattice-engines.com");
            Assert.assertEquals(account.getDuns(), String.format("%09d", idx));
            System.out.println(JsonUtils.serialize(account));
        }
    }

    private void setupMethod(String recordType) throws Exception {
        repo = String.format("%s_%s_%s", leEnv, leStack, "testRepo");
        String tableName = DynamoDataStoreImpl.buildTableName(repo, recordType);

        HdfsUtils.rmdir(yarnConfiguration, sourceDir);
        HdfsUtils.rmdir(yarnConfiguration, targetDir);
        HdfsUtils.mkdir(yarnConfiguration, sourceDir);

        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 10, "Id", ScalarAttributeType.S.name(), null, null);
        client = dynamoService.getClient();
        ListTablesResult result = client.listTables();
        log.info("Tables: " + result.getTableNames());
    }

    private ExportContext submitExport(Class<?> entityClz, String recordType) {
        ExportContext ctx = new ExportContext(yarnConfiguration);

        ExportConfiguration fileExportConfig = new ExportConfiguration();
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

        props.put(DynamoExportJob.CONFIG_REPOSITORY, repo);
        props.put(DynamoExportJob.CONFIG_RECORD_TYPE, recordType);
        props.put(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME, entityClz.getName());
        props.put(DynamoExportJob.CONFIG_ENDPOINT, endpoint);

        fileExportConfig.setProperties(props);
        exportService.exportDataFromHdfs(fileExportConfig, ctx);
        return ctx;
    }

    private Table createLatticeAccountTable() throws IOException {
        Table table = new Table();
        table.setName("LatticeAccount");
        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"LatticeAccount\",\"doc\":\"Testing data\"," + "\"fields\":["
                        + "{\"name\":\"" + LatticeAccount.LATTICE_ACCOUNT_ID_HDFS + "\",\"type\":[\"long\",\"null\"]},"
                        + "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]},"
                        + "{\"name\":\"DUNS\",\"type\":[\"string\",\"null\"]}" + "]}");

        List<GenericRecord> recordList = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            List<Object> tuple = Arrays.asList(Long.valueOf(String.valueOf(i)),
                    String.valueOf(i) + "@lattice-engines.com", "123456789");
            data.add(tuple);
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (List<Object> tuple : data) {
            builder.set(LatticeAccount.LATTICE_ACCOUNT_ID_HDFS, tuple.get(0));
            builder.set("Domain", tuple.get(1));
            builder.set("DUNS", tuple.get(2));
            recordList.add(builder.build());
        }
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, sourceFilePath, recordList);

        Attribute attribute = new Attribute();
        attribute.setName(LatticeAccount.LATTICE_ACCOUNT_ID_HDFS);
        attribute.setDataType("Long");
        table.addAttribute(attribute);

        attribute = new Attribute();
        attribute.setName("Domain");
        attribute.setDataType("String");
        table.addAttribute(attribute);

        attribute = new Attribute();
        attribute.setName("DUNS");
        attribute.setDataType("String");
        table.addAttribute(attribute);

        return table;
    }

    private Table createAccountLookupTable() throws IOException {
        Table table = new Table();
        table.setName("LatticeAccount");
        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"AccountMasterLookup\",\"doc\":\"Testing data\","
                        + "\"fields\":[" + "{\"name\":\"" + AccountLookupEntry.LATTICE_ACCOUNT_ID_HDFS
                        + "\",\"type\":[\"long\",\"null\"]}," + "{\"name\":\"Key\",\"type\":[\"string\",\"null\"]}"
                        + "]}");

        List<GenericRecord> recordList = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            List<Object> tuple = Arrays.asList(Long.valueOf(String.valueOf(i)),
                    String.format("_DOMAIN_lattice-engines.com_DUNS_%09d", i));
            data.add(tuple);
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (List<Object> tuple : data) {
            builder.set(AccountLookupEntry.LATTICE_ACCOUNT_ID_HDFS, tuple.get(0));
            builder.set("Key", tuple.get(1));
            recordList.add(builder.build());
        }
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, sourceFilePath, recordList);

        Attribute attribute = new Attribute();
        attribute.setName(AccountLookupEntry.LATTICE_ACCOUNT_ID_HDFS);
        attribute.setDataType("Long");
        table.addAttribute(attribute);

        attribute = new Attribute();
        attribute.setName("Key");
        attribute.setDataType("String");
        table.addAttribute(attribute);

        return table;
    }

    private class TestLatticeAccountEntityMgrImpl extends BaseFabricEntityMgrImpl<LatticeAccount> {
        TestLatticeAccountEntityMgrImpl() {
            super(new BaseFabricEntityMgrImpl.Builder() //
                    .dataService(fabricDataService) //
                    .recordType(LATTICE_ACCOUNT) //
                    .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                    .repository(repo));
        }
    }

    private class TestAccountLookupEntityMgrImpl extends BaseFabricEntityMgrImpl<AccountLookupEntry> {
        TestAccountLookupEntityMgrImpl() {
            super(new BaseFabricEntityMgrImpl.Builder() //
                    .dataService(fabricDataService) //
                    .recordType(ACCOUNT_LOOKUP_ENTRY) //
                    .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                    .repository(repo));
        }
    }

}
