package com.latticeengines.elasticsearch.service;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.framework.ElasticSearchFunctionalTestNGBase;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;

public class ElasticSearchServiceImplTestNG extends ElasticSearchFunctionalTestNGBase {

    @Inject
    private ElasticSearchService elasticSearchService;

    private ElasticSearchConfig esConfigs;

    @BeforeMethod(groups = { "functional" })
    public void setup() throws Exception {
        esConfigs = elasticSearchService.getDefaultElasticSearchConfig();

    }

    @Test(groups = "functional")
    private void testIndexCRUD() {
        String version = ElasticSearchUtils.generateNewVersion();
        String indexName = String.format("%s_%s_%s", "tenant", TableRoleInCollection.TimelineProfile.name().toLowerCase(),
                version);

        boolean value = elasticSearchService.createIndex(indexName, TableRoleInCollection.TimelineProfile.name());
        Assert.assertTrue(value);

        RetryTemplate retry = RetryUtils.getRetryTemplate(2, Collections.singleton(AssertionError.class), null);
        retry.execute(context ->{
            boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
            Assert.assertTrue(exists);
            return true;
        });

        retry.execute(context -> {
            Map<String, Object> mappings = elasticSearchService.getSourceMapping(indexName);
            verifyField(mappings, "AccountId", "keyword");
            verifyField(mappings, "ContactId", "keyword");
            verifyField(mappings, "EventTimestamp", "date");
            return true;
        });

        elasticSearchService.updateIndexMapping(indexName, TableRoleInCollection.AccountLookup.name(), "nested");

        retry.execute(context -> {
            boolean exists = elasticSearchService.checkFieldExist(indexName,
                    TableRoleInCollection.AccountLookup.name());
            Assert.assertTrue(exists);
            return true;
        });

        retry.execute(context ->{
            boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
            Assert.assertTrue(exists);
            return true;

        });


        elasticSearchService.deleteIndex(indexName);
        retry.execute(context -> {
            boolean exists = elasticSearchService.indexExists(indexName);
            Assert.assertFalse(exists);
            return true;
        });


    }

    @Test(groups = "functional")
    private void testIndexAccount() {
        String version = ElasticSearchUtils.generateNewVersion();
        String indexName = ElasticSearchUtils.constructIndexName("tenant", TableRoleInCollection.AccountLookup.name()
                , version);
        elasticSearchService.createAccountIndexWithLookupIds(indexName, esConfigs, Collections.EMPTY_LIST);

        String columnName = TableRoleInCollection.AccountLookup.name();
        boolean updated = elasticSearchService.updateAccountIndexMapping(indexName,
                columnName,
                "nested", Arrays.asList("AtlasAccountId"), "keyword");
        Assert.assertTrue(updated);
        Map<String, Map<String, String>> map = new HashMap<>();
        map.put(columnName, new HashMap<>());
        map.get(columnName).put("AtlasAccountId", "testAccount");
        boolean created = elasticSearchService.createDocument(indexName, "1", JsonUtils.serialize(map));
        Assert.assertTrue(created);
        // normal case
        String id = elasticSearchService.searchAccountIdByLookupId(indexName, "AtlasAccountId", "testAccount");
        Assert.assertEquals(id, "1");

        // upper case
        String id2 = elasticSearchService.searchAccountIdByLookupId(indexName, "AtlasAccountId", "TESTACCOUNT");
        Assert.assertEquals(id2, "1");


    }


    private void verifyField(Map<String, Object> mappings, String field, String type) {
        Map<?, ?> val = (Map<?, ?>)mappings.get(field);
        Assert.assertNotNull(val);
        Map<String, String> accountMap = JsonUtils.convertMap(val, String.class, String.class);
        Assert.assertEquals(accountMap.get("type"), type);
    }

}
