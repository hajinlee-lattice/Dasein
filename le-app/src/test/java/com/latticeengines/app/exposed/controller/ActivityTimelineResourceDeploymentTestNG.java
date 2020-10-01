package com.latticeengines.app.exposed.controller;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.latticeengines.app.testframework.AppDeploymentTestNGBase;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

public class ActivityTimelineResourceDeploymentTestNG extends AppDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineResourceDeploymentTestNG.class);

    @Inject
    private DynamoItemService dynamoItemService;

    @Inject
    private TimeLineProxy timeLineProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Autowired
    protected Oauth2RestApiProxy oauth2RestApiProxy;

    private OAuth2RestTemplate oAuth2RestTemplate;

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Value("${eai.export.dynamo.timeline.signature}")
    private String activity_signature;

    private final RestTemplate plsRestTemplate = HttpClientUtils.newRestTemplate();
    private final String ACTIVITY_TABLE_NAME = "_REPO_GenericTable_RECORD_GenericTableActivity_";
    private final String ENTITY_TABLE_NAME = "_REPO_GenericTable_RECORD_GenericTableEntity_";
    private final DataCollection.Version DATA_COLLECTION_VERSION = DataCollection.Version.Blue;
    private final String TEST_TENANT_NAME = "LETest1590612472260";
    private final boolean USE_EXISTING_TENANT = true;
    private final String TEST_ACCOUNT_ID = "v5k5xq52updfo67n";
    private final String CLIENT_ID = "playmaker";

    @BeforeClass(groups = "deployment", enabled = false)
    public void setup() throws Exception {

        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(TEST_TENANT_NAME);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
            mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        } else {
            // Create and setup tenant
            Map<String, Boolean> featureFlagMap = new HashMap<>();
            featureFlagMap.put(LatticeFeatureFlag.ENABLE_ACCOUNT360.getName(), true);
            featureFlagMap.putIfAbsent(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
            setupTestEnvironmentWithOneTenant(featureFlagMap);
            setupDataCollection();
            setupAccountLookupData();
            setupActivityTimelineData();
        }

        PlaymakerTenant oAuthTenant = getTenant();
        oauth2RestApiProxy.createTenant(oAuthTenant);

        Thread.sleep(500); // wait for replication lag

        String oneTimeKey = oauth2RestApiProxy.createAPIToken(mainTestCustomerSpace.getTenantId());

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, mainTestCustomerSpace.getTenantId(), oneTimeKey,
                CLIENT_ID);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
    }

    @Test(groups = "deployment", enabled = false)
    public void testActivityTimelineByAccount() {
        DataPage data = oAuth2RestTemplate.getForObject( //
                getUlyssesRestAPIHostPort() + "/ulysses/activity-timeline/accounts/" + TEST_ACCOUNT_ID, //
                DataPage.class);
        Assert.assertNotNull(data);
        Assert.assertTrue(CollectionUtils.isNotEmpty(data.getData()));
    }

    @Test(groups = "deployment", enabled = false)
    public void testActivityTimelineMetrics() {
        Map<String, Integer> metrics = oAuth2RestTemplate.getForObject(
                getUlyssesRestAPIHostPort() + "/ulysses/activity-timeline/accounts/" + TEST_ACCOUNT_ID + "/metrics",
                Map.class);
        Assert.assertNotNull(metrics);
    }

    private void setupDataCollection() {
        DataCollectionStatus dcs = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestCustomerSpace.getTenantId(), DATA_COLLECTION_VERSION);
        dcs.setVersion(DATA_COLLECTION_VERSION);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestCustomerSpace.getTenantId(), dcs,
                dcs.getVersion());
    }

    private void setupAccountLookupData() {
        // Register Account Lookup table
        String accountLookupTableName = "testtable";
        metadataProxy.createTable(mainTestCustomerSpace.getTenantId(), accountLookupTableName,
                getTable(accountLookupTableName));
        // Register Role
        dataCollectionProxy.upsertTable(mainTestCustomerSpace.toString(), accountLookupTableName,
                TableRoleInCollection.AccountLookup, DATA_COLLECTION_VERSION);

        // Register dataunit
        dataUnitProxy.create(mainTestCustomerSpace.getTenantId(),
                generateDynamoDataUnit(mainTestCustomerSpace.getTenantId(), accountLookupTableName));

        // Setup Data
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader
                .getResourceAsStream("com/latticeengines/app/exposed/controller/test-account-lookup-dynamo-items.json");

        List<?> raw = JsonUtils.deserialize(dataStream, List.class);
        List<Item> items = JsonUtils.convertList(raw, String.class).stream()
                .map(itemStr -> String.format(itemStr, mainTestCustomerSpace.getTenantId(), accountLookupTableName))
                .limit(2) //
                .map(Item::fromJSON).map(item -> setAttribute(item)).collect(Collectors.toList());

        String tableName = ENTITY_TABLE_NAME + signature;
        dynamoItemService.batchWrite(tableName, items);
    }

    private Item setAttribute(Item item) {
        try {
            return item.with("Record", Snappy.compress(((String) item.get("Record")).getBytes()));
        } catch (Exception e) {
            log.warn("Attribute set failure.");
            return null;
        }
    }

    private DataUnit generateDynamoDataUnit(String tenantId, String tableName) {
        return JsonUtils.deserialize(
                String.format("{\n" + "        \"StorageType\": \"Dynamo\",\n" + "        \"Tenant\": \"%s\",\n"
                        + "        \"Name\": \"%s\",\n" + "        \"Coalesce\": false,\n"
                        + "        \"Signature\": \"%s\",\n" + "        \"PartitionKey\": \"AtlasLookupKey\",\n"
                        + "        \"StorageType\": \"Dynamo\"\n" + "    }", tenantId, tableName, signature),
                DynamoDataUnit.class);
    }

    private Table getTable(String tableName) {
        return JsonUtils.deserialize(String.format("{\n" + "    \"name\": \"%s\",\n"
                + "    \"display_name\": \"topLevelRecord\",\n" + "    \"attributes\": [\n" + "        {\n"
                + "            \"name\": \"AccountId\",\n" + "            \"display_name\": \"AccountId\",\n"
                + "            \"nullable\": false,\n" + "            \"physical_data_type\": \"string\",\n"
                + "            \"enum_values\": \"\",\n" + "            \"properties\": {},\n"
                + "            \"validatorWrappers\": []\n" + "        },\n" + "        {\n"
                + "            \"name\": \"AtlasLookupKey\",\n" + "            \"display_name\": \"AtlasLookupKey\",\n"
                + "            \"nullable\": false,\n" + "            \"physical_data_type\": \"string\",\n"
                + "            \"enum_values\": \"\",\n" + "            \"properties\": {},\n"
                + "            \"validatorWrappers\": []\n" + "        }\n" + "    ],\n" + "    \"primary_key\": {\n"
                + "        \"name\": \"AtlasLookupKey\",\n" + "        \"display_name\": \"AtlasLookupKey\",\n"
                + "        \"attributes\": [\n" + "            \"AtlasLookupKey\"\n" + "        ]\n" + "    },\n"
                + "    \"created\": 1588772588000,\n" + "    \"updated\": 1588778156000,\n"
                + "    \"retentionPolicy\": \"KEEP_FOREVER\"\n" + "}", tableName), Table.class);
    }

    private void setupActivityTimelineData() {

        // Create Default Timeline
        timeLineProxy.createDefaultTimeLine(mainTestCustomerSpace.getTenantId());
        TimeLine accTl = timeLineProxy.findByEntity(mainTestCustomerSpace.getTenantId(), BusinessEntity.Account);

        // Register Timeline in DatacollectionStatus
        DataCollectionStatus dcs = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestCustomerSpace.getTenantId(), DATA_COLLECTION_VERSION);
        dcs.setVersion(DATA_COLLECTION_VERSION);
        String version = String.valueOf(Instant.now().toEpochMilli());
        Map<String, String> timelineVersionMap = new HashMap<>();
        timelineVersionMap.put(accTl.getTimelineId(), version);
        dcs.setTimelineVersionMap(timelineVersionMap);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestCustomerSpace.getTenantId(), dcs,
                dcs.getVersion());

        // Setup Data
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(
                "com/latticeengines/app/exposed/controller/test-activity-timeline-dynamo-items.json");
        List<?> raw = JsonUtils.deserialize(dataStream, List.class);
        List<Item> items = JsonUtils.convertList(raw, String.class).stream()
                .map(itemStr -> String.format(itemStr, accTl.getTimelineId(), version, getRandomTimestamp())).limit(2) //
                .map(Item::fromJSON).map(item -> item.with("Record", ((String) item.get("Record")).getBytes()))
                .collect(Collectors.toList());

        String tableName = ACTIVITY_TABLE_NAME + activity_signature;
        dynamoItemService.batchWrite(tableName, items);
    }

    private String getRandomTimestamp() {
        long startSeconds = Instant.now().minus(Duration.ofDays(90)).getEpochSecond();
        long endSeconds = Instant.now().getEpochSecond();
        return String.valueOf(ThreadLocalRandom.current().nextLong(startSeconds, endSeconds));
    }

    private PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setTenantName(mainTestCustomerSpace.getTenantId());
        tenant.setExternalId(CLIENT_ID);
        tenant.setJdbcDriver("");
        tenant.setJdbcUrl("");
        return tenant;
    }
}
