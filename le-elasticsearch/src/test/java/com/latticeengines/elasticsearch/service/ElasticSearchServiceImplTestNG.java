package com.latticeengines.elasticsearch.service;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Base64Utils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.framework.ElasticSearchFunctionalTestNGBase;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;

public class ElasticSearchServiceImplTestNG extends ElasticSearchFunctionalTestNGBase {

    @Inject
    private ElasticSearchService elasticSearchService;

    private ElasticSearchConfig esConfigs;

    @BeforeMethod(groups = { "functional" })
    @Override
    public void setup() throws Exception {
        esConfigs = elasticSearchService.getDefaultElasticSearchConfig();

    }

    @Test(groups = "functional")
    private void testIndexTimeline() {
        String version = ElasticSearchUtils.generateNewVersion();
        String indexName = ElasticSearchUtils.constructIndexName("tenant",
                TableRoleInCollection.TimelineProfile.name(), version);

        boolean value = elasticSearchService.createIndex(indexName, TableRoleInCollection.TimelineProfile.name());
        Assert.assertTrue(value);

        RetryTemplate retry = RetryUtils.getRetryTemplate(2, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
            Assert.assertTrue(exists);
            return true;
        });

        // check field mapping
        retry.execute(context -> {
            Map<String, Object> mappings = elasticSearchService.getSourceMapping(indexName);
            verifyField(mappings, InterfaceName.AccountId.name(), "keyword");
            verifyField(mappings, ContactId.name(), "keyword");
            verifyField(mappings, TimeLineStoreUtils.TimelineStandardColumn.EventDate.getColumnName(), "date");
            return true;
        });

        // update index mapping
        elasticSearchService.updateIndexMapping(indexName, TableRoleInCollection.AccountLookup.name(), "nested");

        // check new field exist
        retry.execute(context -> {
            boolean exists = elasticSearchService.checkFieldExist(indexName,
                    TableRoleInCollection.AccountLookup.name());
            Assert.assertTrue(exists);
            return true;
        });

        // check existing field exists
        retry.execute(context -> {
            boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
            Assert.assertTrue(exists);
            return true;

        });

        // create document
        Map<String, String> doc = new HashMap<>();
        doc.put(InterfaceName.AccountId.name(), "account1");
        doc.put(ContactId.name(), "contact1");
        doc.put(TimeLineStoreUtils.TimelineStandardColumn.EventDate.getColumnName(), "1607217840000");
        boolean created = elasticSearchService.createDocument(indexName, "1", JsonUtils.serialize(doc));
        Assert.assertTrue(created);
        // wait refresh time
        SleepUtils.sleep(61000);
        // search account id
        List<Map<String, Object>> accountResults = elasticSearchService.searchTimelineByEntityIdAndDateRange(indexName,
                BusinessEntity.Account.name(),
                "account1", 1607217830000L, 1607217880000L);
        Assert.assertEquals(accountResults.size(), 1);
        Map<String, Object> accountResult = accountResults.get(0);
        Assert.assertTrue(accountResult.containsKey(AccountId.name()));
        Assert.assertEquals(accountResult.get(AccountId.name()).toString(), "account1");
        Assert.assertTrue(accountResult.containsKey(ContactId.name()));
        Assert.assertEquals(accountResult.get(ContactId.name()).toString(), "contact1");

        // search contact id
        List<Map<String, Object>> contactResults = elasticSearchService.searchTimelineByEntityIdAndDateRange(indexName,
                BusinessEntity.Contact.name(),
                "contact1", 1607217830000L, 1607217880000L);
        Assert.assertEquals(contactResults.size(), 1);
        Map<String, Object> contactResult = accountResults.get(0);
        Assert.assertTrue(contactResult.containsKey(AccountId.name()));
        Assert.assertEquals(contactResult.get(AccountId.name()).toString(), "account1");
        Assert.assertTrue(contactResult.containsKey(ContactId.name()));
        Assert.assertEquals(contactResult.get(ContactId.name()).toString(), "contact1");

        // remove the index
        elasticSearchService.deleteIndex(indexName);
        retry.execute(context -> {
            boolean exists = elasticSearchService.indexExists(indexName);
            Assert.assertFalse(exists);
            return true;
        });


    }

    @Test(groups ="functional")
    private void testIndexContact() throws IOException {
        String version = ElasticSearchUtils.generateNewVersion();
        String contactIndex = ElasticSearchUtils.constructIndexName("tenant",
                BusinessEntity.Contact.name(), version);
        elasticSearchService.createIndex(contactIndex, BusinessEntity.Contact.name());

        // create contact document
        String columnName = TableRoleInCollection.ConsolidatedContact.name();
        elasticSearchService.updateIndexMapping(contactIndex, columnName, "binary");
        Map<String, Object> doc = new HashMap<>();
        Map<String, String> contacts = new HashMap<>();
        contacts.put(InterfaceName.ContactName.name(), "Wilhelm Darnbrough");
        contacts.put(InterfaceName.FirstName.name(), "Wilhelm");
        contacts.put(InterfaceName.LastName.name(), "Darnbrough");
        contacts.put(InterfaceName.NumberOfEmployees.name(), "1233");
        doc.put(columnName, Snappy.compress(JsonUtils.serialize(contacts)));
        doc.put(AccountId.name(), "account1");
        boolean created = elasticSearchService.createDocument(contactIndex, "contact1", JsonUtils.serialize(doc));
        Assert.assertTrue(created);

        // wait the refresh interval
        SleepUtils.sleep(61000);

        // search the document by account id
        List<Map<String, Object>> results = elasticSearchService.searchContactByAccountId(contactIndex, "account1");
        Assert.assertEquals(results.size(), 1);
        Map<String, Object> searchResult = results.get(0);
        Assert.assertTrue(searchResult.containsKey(AccountId.name()));
        Assert.assertEquals(searchResult.get(AccountId.name()), "account1");
        Assert.assertTrue(searchResult.containsKey(columnName));
        Map<String, String> searchContacts =
                JsonUtils.deserialize(
                        new String(
                                Snappy.uncompress(
                                        Base64Utils.decode(
                                                String.valueOf(searchResult.get(columnName)).getBytes()))),
                        new TypeReference<Map<String, String>>() {});
        Assert.assertEquals(searchContacts.get(InterfaceName.ContactName.name()), "Wilhelm Darnbrough");
        Assert.assertEquals(searchContacts.get(InterfaceName.FirstName.name()), "Wilhelm");
        Assert.assertEquals(searchContacts.get(InterfaceName.LastName.name()), "Darnbrough");
        Assert.assertEquals(searchContacts.get(InterfaceName.NumberOfEmployees.name()), "1233");

        // search the document by contact id
        List<Map<String, Object>> results1 = elasticSearchService.searchContactByContactId(contactIndex, "contact1");
        Assert.assertEquals(results.size(), 1);
        Map<String, Object> searchResult1 = results1.get(0);
        Assert.assertTrue(searchResult1.containsKey(AccountId.name()));
        Assert.assertEquals(searchResult1.get(AccountId.name()), "account1");
        Assert.assertTrue(searchResult1.containsKey(columnName));
        Map<String, String> searchContacts1 =
                JsonUtils.deserialize(
                        new String(
                                Snappy.uncompress(
                                        Base64Utils.decode(
                                                String.valueOf(searchResult1.get(columnName)).getBytes()))),
                        new TypeReference<Map<String, String>>() {});
        Assert.assertEquals(searchContacts1.get(InterfaceName.ContactName.name()), "Wilhelm Darnbrough");
        Assert.assertEquals(searchContacts1.get(InterfaceName.FirstName.name()), "Wilhelm");
        Assert.assertEquals(searchContacts1.get(InterfaceName.LastName.name()), "Darnbrough");
        Assert.assertEquals(searchContacts1.get(InterfaceName.NumberOfEmployees.name()), "1233");

        // remove the index
        elasticSearchService.deleteIndex(contactIndex);
        RetryTemplate retry = RetryUtils.getRetryTemplate(2, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            boolean exists = elasticSearchService.indexExists(contactIndex);
            Assert.assertFalse(exists);
            return true;
        });
    }


    @Test(groups = "functional")
    private void testIndexAccount() {
        String version = ElasticSearchUtils.generateNewVersion();
        String accountIndex = ElasticSearchUtils.constructIndexName("tenant", TableRoleInCollection.AccountLookup.name()
                , version);
        elasticSearchService.createAccountIndexWithLookupIds(accountIndex, esConfigs, Collections.EMPTY_LIST);

        String columnName = TableRoleInCollection.AccountLookup.name();
        boolean updated = elasticSearchService.updateAccountIndexMapping(accountIndex,
                columnName,
                "nested", Arrays.asList("AtlasAccountId"), "keyword");
        Assert.assertTrue(updated);
        Map<String, Map<String, String>> doc = new HashMap<>();
        doc.put(columnName, new HashMap<>());
        doc.get(columnName).put("AtlasAccountId", "testAccount");
        doc.get(columnName).put(AccountId.name(), "1");
        boolean created = elasticSearchService.createDocument(accountIndex, "1", JsonUtils.serialize(doc));
        Assert.assertTrue(created);
        // wait refresh time
        SleepUtils.sleep(61000);

        // normal case, search account id
        String id = elasticSearchService.searchAccountIdByLookupId(accountIndex, "AtlasAccountId", "testAccount");
        Assert.assertEquals(id, "1");

        // upper case
        String id2 = elasticSearchService.searchAccountIdByLookupId(accountIndex, "AtlasAccountId", "TESTACCOUNT");
        Assert.assertEquals(id2, "1");

        // lowercase
        String id3 = elasticSearchService.searchAccountIdByLookupId(accountIndex, "AtlasAccountId", "testaccount");
        Assert.assertEquals(id3, "1");



        // search account by lookup id
        Map<String, Object> result1 = elasticSearchService.searchByLookupId(accountIndex, "AtlasAccountId",
                "testAccount");
        Assert.assertTrue(result1.containsKey(columnName));
        Map<String, String> doc1 = JsonUtils.deserialize(JsonUtils.serialize(result1.get(columnName)),
                new TypeReference<Map<String, String>>() {});
        Assert.assertEquals(doc1.get(AccountId.name()), "1");
        Assert.assertEquals(doc1.get("AtlasAccountId"), "testAccount");


        // search account by account id
        Map<String, Object> result2 = elasticSearchService.searchByAccountId(accountIndex, "1");
        Map<String, String> doc2 = JsonUtils.deserialize(JsonUtils.serialize(result2.get(columnName)),
                new TypeReference<Map<String, String>>() {});
        Assert.assertEquals(doc2.get(AccountId.name()), "1");
        Assert.assertEquals(doc2.get("AtlasAccountId"), "testAccount");


        elasticSearchService.deleteIndex(accountIndex);

        RetryTemplate retry = RetryUtils.getRetryTemplate(2, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            boolean exists = elasticSearchService.indexExists(accountIndex);
            Assert.assertFalse(exists);
            return true;
        });
    }

    private void verifyField(Map<String, Object> mappings, String field, String type) {
        Map<?, ?> val = (Map<?, ?>)mappings.get(field);
        Assert.assertNotNull(val);
        Map<String, String> accountMap = JsonUtils.convertMap(val, String.class, String.class);
        Assert.assertEquals(accountMap.get("type"), type);
    }

}
