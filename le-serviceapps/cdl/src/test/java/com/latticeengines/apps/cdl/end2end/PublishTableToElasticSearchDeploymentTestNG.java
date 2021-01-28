package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.apps.cdl.end2end.ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Base64Utils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.ElasticSearchDataUnit;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PublishTableProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;

public class PublishTableToElasticSearchDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PublishTableToElasticSearchDeploymentTestNG.class);


    @Inject
    private PublishTableProxy publishTableProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private ElasticSearchService elasticSearchService;

    @Inject
    private TimeLineProxy timeLineProxy;

    @Inject
    private ActivityProxy activityProxy;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.putIfAbsent(LatticeFeatureFlag.QUERY_FROM_ELASTICSEARCH.getName(), true);
        super.setupEnd2EndTestEnvironment(featureFlagMap);
        // account/contact in check point, not time line profile
        resumeCheckpoint(CHECK_POINT);
    }

    @Test(groups = "end2end")
    public void testTimelineProfile() throws Exception {

        PublishTableToESRequest request = generateTimelineRequest(TableRoleInCollection.TimelineProfile);
        String appId = publishTableProxy.publishTableToES(mainCustomerSpace, request);
        JobStatus status = waitForWorkflowStatus(appId, false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        // wait the refresh interval
        SleepUtils.sleep(61000);
        // fake data for query
        prepareTimeline();

        String indexName = getIndexName(TimelineProfile);


        // get field mapping from index and assert field exists
        // class not define error(MappingMetadata)
        // todo resolve the maven conflict elasticsearch 7.9.1 vs 7.6.2
        //Map<String, Object> mappings = elasticSearchService.getSourceMapping(indexName);
        //verifyField(mappings, "AccountId", "keyword");
        //verifyField(mappings, "ContactId", "keyword");
        //verifyField(mappings, "EventTimestamp", "date");

        // query from elastic search
        ActivityTimelineQuery query = new ActivityTimelineQuery();
        query.setMainEntity(BusinessEntity.Contact);
        query.setEntityId("Alert020Contact001");
        query.setStartTimeStamp(Instant.ofEpochMilli(1607217830000L));
        query.setEndTimeStamp(Instant.ofEpochMilli(1607217880000L));
        DataPage dataPage = activityProxy.getData(mainCustomerSpace, null, query);
        Assert.assertTrue(dataPage.getData().size() > 0);

        deleteIndex(indexName);

    }

    @Test(groups = "end2end")
    private void testPublishAccount() {

        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, ConsolidatedAccount);
        PublishTableToESRequest request = generateRequest(ConsolidatedAccount, tableName);
        String appId = publishTableProxy.publishTableToES(mainCustomerSpace, request);
        JobStatus status = waitForWorkflowStatus(appId, false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        // wait the refresh interval
        SleepUtils.sleep(61000);

        String indexName = getIndexName(ConsolidatedAccount);
        Map<String, Object> result = matchProxy.lookupAccount(mainCustomerSpace, indexName,
                AccountId.name(), "898");

        Assert.assertEquals(result.get(AccountId.name()).toString(), "898");

    }

    @Test(groups = "end2end")
    private void testPublishContact() throws IOException {

        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, ConsolidatedContact);
        PublishTableToESRequest request = generateRequest( ConsolidatedContact, tableName);
        String appId = publishTableProxy.publishTableToES(mainCustomerSpace, request);
        JobStatus status = waitForWorkflowStatus(appId, false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        // wait the refresh interval
        SleepUtils.sleep(61000);

        String indexName = getIndexName( ConsolidatedContact);

        // search contacts by account id
        List<Map<String, Object>> contacts = matchProxy.lookupContacts(mainCustomerSpace,
                AccountId.name(), "898", "",
                null);

        Assert.assertTrue(CollectionUtils.isNotEmpty(contacts));
        Map<String, Object> result = contacts.get(0);
        Assert.assertTrue(result.containsKey(ConsolidatedContact.name()));

        // get the column value, then verify the key in map
        Map<String, String> recordMap =
                JsonUtils.deserialize(
                        new String(
                                Snappy.uncompress(
                                        Base64Utils.decode(
                                                String.valueOf(result.get(ConsolidatedContact.name())).getBytes()))),
                        new TypeReference<Map<String, String>>() {});

        Assert.assertTrue(recordMap.containsKey(AccountId.name()));
        Assert.assertTrue(recordMap.containsKey(InterfaceName.ContactId.name()));

        // search contacts by contact id
        List<Map<String, Object>> contacts1 = matchProxy.lookupContacts(mainCustomerSpace,
                null, null,
                "5ae2bdccfc13ae3162000382", null);
        Map<String, Object> result1 = contacts.get(0);

        Map<String, String> recordMap1 =
                JsonUtils.deserialize(
                        new String(
                                Snappy.uncompress(
                                        Base64Utils.decode(
                                                String.valueOf(result1.get(ConsolidatedContact.name())).getBytes()))),
                        new TypeReference<Map<String, String>>() {});

        Assert.assertTrue(recordMap1.containsKey(AccountId.name()));
        Assert.assertTrue(recordMap1.containsKey(InterfaceName.ContactId.name()));


        deleteIndex(indexName);
    }

    // share the same index with consolidate account
    @Test(groups = "end2end", dependsOnMethods = "testPublishAccount")
    private void testPublishAccountLookup() throws IOException {

        // prepare lookup id
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("user_SalesforceSandboxAccountID");
        crmIds.add("user_SalesforceAccountID");
        crmIds.add("user_SalesforceSandboxAccountID");
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setEntity(BusinessEntity.Account);

        cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(mainCustomerSpace, cdlExternalSystem);


        // user_SalesforceSandboxAccountID user_SalesforceAccountID user_SalesforceSandboxAccountID
        String tableName = setupTables(TableRoleInCollection.AccountLookup);
        PublishTableToESRequest request = generateRequest(TableRoleInCollection.AccountLookup, tableName);
        String appId = publishTableProxy.publishTableToES(mainCustomerSpace, request);
        JobStatus status = waitForWorkflowStatus(appId, false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        // wait the refresh interval
        SleepUtils.sleep(61000);

        String indexName = getIndexName(TableRoleInCollection.AccountLookup);
        // user_MarketoAccountID
        String value = matchProxy.lookupInternalAccountId(mainCustomerSpace, "user_MarketoAccountID",
                "0014p000028blgqqa0",
                null);
        Assert.assertEquals(value, "898");

        // user_SalesforceAccountID
        String value1 = matchProxy.lookupInternalAccountId(mainCustomerSpace, "user_SalesforceAccountID",
                "0014p000028blgqqa0",
                null);
        Assert.assertEquals(value1, "898");


        // user_SalesforceSandboxAccountID
        String value2 = matchProxy.lookupInternalAccountId(mainCustomerSpace, "user_SalesforceSandboxAccountID",
                "0015p000028blgqqa0",
                null);
        Assert.assertEquals(value2, "898");
        deleteIndex(indexName);

    }



    private String getIndexName(TableRoleInCollection role) {
        String entity = ElasticSearchUtils.getEntityFromTableRole(role);
        ElasticSearchDataUnit unit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(mainCustomerSpace,
                entity,
                DataUnit.StorageType.ElasticSearch);
        Assert.assertNotNull(unit);
        Assert.assertTrue(StringUtils.isNotBlank(unit.getSignature()));
        String signature = unit.getSignature();
        String indexName = ElasticSearchUtils.constructIndexName(CustomerSpace.shortenCustomerSpace(mainCustomerSpace),
                entity, signature);

        RetryTemplate retry = RetryUtils.getRetryTemplate(3, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            Assert.assertTrue(elasticSearchService.indexExists(indexName));
            return true;
        });
        return indexName;
    }

    private void deleteIndex(String indexName) {
        elasticSearchService.deleteIndex(indexName);

        RetryTemplate retry = RetryUtils.getRetryTemplate(3, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            Assert.assertFalse(elasticSearchService.indexExists(indexName));
            return true;
        });
    }

    private void verifyField(Map<String, Object> mappings, String field, String type) {
        Map<?, ?> val = (Map<?, ?>)mappings.get(field);
        Assert.assertNotNull(val);
        Map<String, String> accountMap = JsonUtils.convertMap(val, String.class, String.class);
        Assert.assertEquals(accountMap.get("type"), type);
    }

    private PublishTableToESRequest generateTimelineRequest(TableRoleInCollection role) throws IOException {
        String tableName = setupTables(role);
        return generateRequest(role, tableName);
    }

    private PublishTableToESRequest generateRequest(TableRoleInCollection role, String tableName) {
        PublishTableToESRequest request = new PublishTableToESRequest();
        ElasticSearchExportConfig config = new ElasticSearchExportConfig();
        String entity = ElasticSearchUtils.getEntityFromTableRole(role);
        ElasticSearchDataUnit unit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(mainCustomerSpace, entity,
                DataUnit.StorageType.ElasticSearch);
        if (unit != null) {
            config.setSignature(unit.getSignature());
        } else {
            config.setSignature(ElasticSearchUtils.generateNewVersion());
        }
        config.setTableRoleInCollection(role);
        config.setTableName(tableName);
        List<ElasticSearchExportConfig> configs = Collections.singletonList(config);
        request.setExportConfigs(configs);

        ElasticSearchConfig esConfig = elasticSearchService.getDefaultElasticSearchConfig();
        String encryptionKey = CipherUtils.generateKey();
        String salt = CipherUtils.generateKey();
        esConfig.setEncryptionKey(encryptionKey);
        esConfig.setSalt(salt);
        esConfig.setEsPassword(CipherUtils.encrypt(esConfig.getEsPassword(), encryptionKey, salt));

        request.setEsConfig(esConfig);
        return request;
    }

    private String setupTables(TableRoleInCollection role) throws IOException {
        Table esTable = JsonUtils
                .deserialize(IOUtils.toString(ClassLoader.getSystemResourceAsStream(
                        String.format("end2end/role/%s.json", role.name().toLowerCase())), "UTF-8"),
                        Table.class);
        String esTableName = NamingUtils.timestamp("es");
        esTable.setName(esTableName);
        Extract extract = esTable.getExtracts().get(0);
        extract.setPath(PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(mainCustomerSpace))
                .append(esTableName).toString()
                + "/*.avro");
        esTable.setExtracts(Collections.singletonList(extract));
        metadataProxy.createTable(mainCustomerSpace, esTableName, esTable);

        String path = ClassLoader
                .getSystemResource("end2end/role").getPath();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, //
                path + String.format("/%s.avro", role.name().toLowerCase()), //
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(),
                                CustomerSpace.parse(mainCustomerSpace))
                        .append(esTableName).append("part1.avro").toString());
        return esTableName;
    }

    private void prepareTimeline() {
        // fake time line
        String timelineName1 = "timelineName1";
        TimeLine timeLine1 = new TimeLine();
        timeLine1.setName(timelineName1);
        String timelineId = String.format("%s_%s", CustomerSpace.shortenCustomerSpace(mainCustomerSpace),
                timelineName1);
        timeLine1.setTimelineId(timelineId);
        timeLine1.setEntity(BusinessEntity.Contact.name());
        timeLine1.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.MarketingActivity.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.MarketingActivity));
        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));

        timeLine1.setEventMappings(mappingMap);
        timeLineProxy.createTimeline(mainCustomerSpace, timeLine1);

        // fake data collection status
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(mainCustomerSpace, version);
        Map<String, String> timelineVersionMap = ImmutableMap.of(timelineId, "timelineVersion");
        status.setTimelineVersionMap(timelineVersionMap);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status, version);
    }

}
