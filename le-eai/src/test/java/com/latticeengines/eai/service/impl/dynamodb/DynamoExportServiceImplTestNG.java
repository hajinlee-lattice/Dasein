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
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.match.LatticeAccount;
import com.latticeengines.eai.dynamodb.runtime.DynamoExportJob;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;
import com.snowflake.client.jdbc.internal.apache.commons.lang3.StringUtils;

public class DynamoExportServiceImplTestNG extends EaiFunctionalTestNGBase {

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("DynamoTestCustomer");
    private AmazonDynamoDBClient client;
    private static final String RECORD_TYPE = "LatticeAccount";

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
    private String tableName;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        repo = String.format("%s_%s_%s", leEnv, leStack, "testRepo");
        tableName = DynamoDataStoreImpl.buildTableName(repo, RECORD_TYPE);

        HdfsUtils.rmdir(yarnConfiguration, sourceDir);
        HdfsUtils.rmdir(yarnConfiguration, targetDir);
        HdfsUtils.mkdir(yarnConfiguration, sourceDir);
        table = createTable();

        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 50, "Id", ScalarAttributeType.S.name(), null, null);
        client = dynamoService.getClient();
        ListTablesResult result = client.listTables();
        log.info("Tables: " + result.getTableNames());
    }

    @AfterClass(groups = "aws")
    public void cleanup() throws IOException {
        dynamoService.deleteTable(tableName);
    }

    @Test(groups = "aws")
    public void exportMetadataAndDataAndWriteToHdfs() throws Exception {
        ExportContext ctx = new ExportContext(yarnConfiguration);

        ExportConfiguration fileExportConfig = new ExportConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.AVRO);
        fileExportConfig.setExportDestination(ExportDestination.DYNAMO);
        fileExportConfig.setCustomerSpace(TEST_CUSTOMER);

        Extract extract = new Extract();
        extract.setPath(sourceFilePath);
        table.setExtracts(Collections.singletonList(extract));

        fileExportConfig.setTable(table);
        fileExportConfig.setExportTargetPath(targetDir);
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.NUM_MAPPERS, "2");

        props.put(DynamoExportJob.CONFIG_REPOSITORY, repo);
        props.put(DynamoExportJob.CONFIG_RECORD_TYPE, RECORD_TYPE);
        props.put(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME, LatticeAccount.class.getName());

        if (StringUtils.isEmpty(endpoint)) {
            props.put(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                    CipherUtils.encrypt(accessKey.replace("\n", "")));
            props.put(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                    CipherUtils.encrypt(secretKey.replace("\n", "")));
        } else {
            props.put(DynamoExportJob.CONFIG_ENDPOINT, endpoint);
        }

        fileExportConfig.setProperties(props);
        exportService.exportDataFromHdfs(fileExportConfig, ctx);

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
            System.out.println(account.toFabricAvroRecord(RECORD_TYPE));
        }
    }

    private Table createTable() throws IOException {
        Table table = new Table();
        table.setName("LatticeAccount");
        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"LatticeAccount\",\"doc\":\"Testing data\"," + "\"fields\":["
                        + "{\"name\":\"LatticeAccountId\",\"type\":[\"string\",\"null\"]},"
                        + "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]},"
                        + "{\"name\":\"DUNS\",\"type\":[\"string\",\"null\"]}" + "]}");

        List<GenericRecord> recordList = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            List<Object> tuple = Arrays.asList((Object) String.valueOf(i), String.valueOf(i) + "@lattice-engines.com",
                    "123456789");
            data.add(tuple);
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (List<Object> tuple : data) {
            builder.set("LatticeAccountId", tuple.get(0));
            builder.set("Domain", tuple.get(1));
            builder.set("DUNS", tuple.get(2));
            recordList.add(builder.build());
        }
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, sourceFilePath, recordList);

        Attribute attribute = new Attribute();
        attribute.setName("LatticeAccountId");
        attribute.setDataType("String");
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

    private class TestLatticeAccountEntityMgrImpl extends BaseFabricEntityMgrImpl<LatticeAccount> {
        TestLatticeAccountEntityMgrImpl() {
            super(new BaseFabricEntityMgrImpl.Builder().messageService(null)
                    .dataService(fabricDataService) //
                    .recordType(RECORD_TYPE).topic(null) //
                    .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository(repo));
        }
    }

}
