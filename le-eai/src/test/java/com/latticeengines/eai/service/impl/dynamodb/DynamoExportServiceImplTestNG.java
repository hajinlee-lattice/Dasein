package com.latticeengines.eai.service.impl.dynamodb;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_REPOSITORY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.dynamodb.runtime.DynamoExportJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.yarn.exposed.service.impl.JobServiceImpl;


/**
 * This test requires a local Dynamo running at port 8000
 */
public class DynamoExportServiceImplTestNG extends EaiMiniClusterFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DynamoExportServiceImplTestNG.class);

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("DynamoTestCustomer");

    private static final String LATTICE_ACCOUNT = LatticeAccount.class.getSimpleName();
    private static final String ACCOUNT_LOOKUP_ENTRY = AccountLookupEntry.class.getSimpleName();

    @Inject
    private DynamoService dynamoService;

    @Resource(name = "dynamoExportService")
    private ExportService exportService;

    @Inject
    private FabricDataService fabricDataService;

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
    private String repo;

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
    public void exportLatticeAccount() throws Exception {
        setupMethod(LATTICE_ACCOUNT);

        table = createLatticeAccountTable();
        submitExport(LatticeAccount.class, LATTICE_ACCOUNT);

        TestLatticeAccountEntityMgrImpl entityMgr = new TestLatticeAccountEntityMgrImpl();
        entityMgr.init();

        for (int i = 0; i < 10; i++) {
            Integer idx = new Random().nextInt(10000);
            LatticeAccount account = entityMgr.findByKey(idx.toString());
            Assert.assertNotNull(account);
            System.out.println(JsonUtils.serialize(account));
        }

        teardownMethod(LATTICE_ACCOUNT);
    }

    @Test(groups = "dynamo")
    public void exportAccountLookup() throws Exception {
        setupMethod(ACCOUNT_LOOKUP_ENTRY);

        table = createAccountLookupTable();
        submitExport(AccountLookupEntry.class, ACCOUNT_LOOKUP_ENTRY);

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

        teardownMethod(ACCOUNT_LOOKUP_ENTRY);
    }

    private void setupMethod(String recordType) throws Exception {
        repo = String.format("%s_%s_%s", leEnv, leStack, "testRepo");
        String tableName = DynamoDataStoreImpl.buildTableName(repo, recordType);

        HdfsUtils.rmdir(miniclusterConfiguration, sourceDir);
        HdfsUtils.rmdir(miniclusterConfiguration, targetDir);
        HdfsUtils.mkdir(miniclusterConfiguration, sourceDir);

        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 10, "Id", ScalarAttributeType.S.name(), null, null, null);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());
    }

    private void teardownMethod(String recordType) throws Exception {
        repo = String.format("%s_%s_%s", leEnv, leStack, "testRepo");
        String tableName = DynamoDataStoreImpl.buildTableName(repo, recordType);
        dynamoService.deleteTable(tableName);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());
    }

    private void submitExport(Class<?> entityClz, String recordType) throws Exception {
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

        props.put(CONFIG_REPOSITORY, repo);
        props.put(CONFIG_RECORD_TYPE, recordType);
        props.put(CONFIG_ENTITY_CLASS_NAME, entityClz.getName());
        props.put(CONFIG_ENDPOINT, endpoint);

        fileExportConfig.setProperties(props);

        Properties properties = ((DynamoExportServiceImpl) exportService).constructProperties(fileExportConfig,
                new ExportContext(miniclusterConfiguration));
        testMRJob(DynamoExportJob.class, properties);
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
        AvroUtils.writeToHdfsFile(miniclusterConfiguration, schema, sourceFilePath, recordList);

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
        AvroUtils.writeToHdfsFile(miniclusterConfiguration, schema, sourceFilePath, recordList);

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
